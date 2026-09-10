-- mart_repo_activity_daily
--
-- Grain: one row per repo x calendar day. The headline "engineering activity"
-- table for the GitHub source: commit volume, issue open/close flow, and PR
-- open/merge flow, plus contributor counts, all on the same date spine so a
-- dashboard can trend them together.
--
-- Note on completeness: counts for a given day are only as complete as the
-- incremental pull that produced them. A day close to "now" may still gain
-- rows on the next run (e.g. an issue reopened and updated); historical days
-- are stable once their activity falls outside every future `since` window.

with commits as (
    select
        repo_full_name,
        committed_date                       as activity_date,
        count(*)                             as commits,
        count(distinct coalesce(author_login, author_email)) as commit_authors
    from {{ ref('stg_github__commits') }}
    where committed_date is not null
    group by 1, 2
),

issues_opened as (
    select repo_full_name, created_date as activity_date,
           count(*) as issues_opened
    from {{ ref('stg_github__issues') }}
    where created_date is not null
    group by 1, 2
),

issues_closed as (
    select repo_full_name, closed_date as activity_date,
           count(*) as issues_closed
    from {{ ref('stg_github__issues') }}
    where closed_date is not null
    group by 1, 2
),

prs_opened as (
    select repo_full_name, created_date as activity_date,
           count(*) as prs_opened
    from {{ ref('stg_github__pull_requests') }}
    where created_date is not null
    group by 1, 2
),

prs_merged as (
    select repo_full_name, merged_date as activity_date,
           count(*)             as prs_merged,
           round(avg(days_to_merge), 2) as avg_days_to_merge
    from {{ ref('stg_github__pull_requests') }}
    where merged_date is not null
    group by 1, 2
),

spine as (
    select repo_full_name, activity_date from commits
    union
    select repo_full_name, activity_date from issues_opened
    union
    select repo_full_name, activity_date from issues_closed
    union
    select repo_full_name, activity_date from prs_opened
    union
    select repo_full_name, activity_date from prs_merged
)

select
    s.repo_full_name,
    s.activity_date,
    coalesce(c.commits, 0)          as commits,
    coalesce(c.commit_authors, 0)   as commit_authors,
    coalesce(io.issues_opened, 0)   as issues_opened,
    coalesce(ic.issues_closed, 0)   as issues_closed,
    coalesce(po.prs_opened, 0)      as prs_opened,
    coalesce(pm.prs_merged, 0)      as prs_merged,
    pm.avg_days_to_merge,
    coalesce(c.commits, 0)
        + coalesce(io.issues_opened, 0)
        + coalesce(po.prs_opened, 0) as total_contributions
from spine s
left join commits c        on c.repo_full_name = s.repo_full_name and c.activity_date = s.activity_date
left join issues_opened io on io.repo_full_name = s.repo_full_name and io.activity_date = s.activity_date
left join issues_closed ic on ic.repo_full_name = s.repo_full_name and ic.activity_date = s.activity_date
left join prs_opened po    on po.repo_full_name = s.repo_full_name and po.activity_date = s.activity_date
left join prs_merged pm    on pm.repo_full_name = s.repo_full_name and pm.activity_date = s.activity_date
