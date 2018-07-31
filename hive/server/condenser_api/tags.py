"""condenser_api trending tag fetching methods"""

from aiocache import cached
from hive.server.condenser_api.common import (valid_tag, valid_limit)

def db_context(function):
    #@wraps(function)
    def _wrapper(*args, **kwargs):
        assert 'db' not in kwargs
        assert 'context' in kwargs
        assert 'db' in kwargs['context']
        context = kwargs.pop('context')
        kwargs['db'] = context['db']
        return function(*args, **kwargs)
    return _wrapper


@cached(ttl=3600)
@db_context
async def get_top_trending_tags_summary(db):
    """Get top 50 trending tags among pending posts."""
    # Same results, more overhead:
    #return [tag['name'] for tag in await get_trending_tags('', 50)]
    sql = """
        SELECT category
          FROM hive_posts_cache
         WHERE is_paidout = '0'
      GROUP BY category
      ORDER BY SUM(payout) DESC
         LIMIT 50
    """
    return db.query_col(sql)

@cached(ttl=3600)
@db_context
async def get_trending_tags(db, start_tag: str = '', limit: int = 250):
    """Get top 250 trending tags among pending posts, with stats."""

    limit = valid_limit(limit, ubound=250)
    start_tag = valid_tag(start_tag or '', allow_empty=True)

    if start_tag:
        seek = """
          HAVING SUM(payout) <= (
            SELECT SUM(payout)
              FROM hive_posts_cache
             WHERE is_paidout = '0'
               AND category = :start_tag)
        """
    else:
        seek = ''

    sql = """
      SELECT category,
             COUNT(*) AS total_posts,
             SUM(CASE WHEN depth = 0 THEN 1 ELSE 0 END) AS top_posts,
             SUM(payout) AS total_payouts
        FROM hive_posts_cache
       WHERE is_paidout = '0'
    GROUP BY category %s
    ORDER BY SUM(payout) DESC
       LIMIT :limit
    """ % seek

    out = []
    for row in db.query_all(sql, limit=limit, start_tag=start_tag):
        out.append({
            'name': row['category'],
            'comments': row['total_posts'] - row['top_posts'],
            'top_posts': row['top_posts'],
            'total_payouts': "%.3f SBD" % row['total_payouts']})

    return out
