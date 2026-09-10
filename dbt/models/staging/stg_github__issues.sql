-- The GitHub issues endpoint returns both issues and pull requests. This model
-- keeps only true issues (is_pull_request = false); PRs are modelled from the
-- dedicated pulls endpoint in stg_github__pull_requests.

with source as (
    select * from {{ source('github', 'issues') }}
),

renamed as (
    select
        repo_full_name,
        number                                 as issue_number,
        id                                     as issue_id,
        title,
        state,
        user_login                             as author_login,
        coalesce(comments, 0)                  as comment_count,
        coalesce(jsonb_array_length(labels), 0) as label_count,
        labels                                 as labels,
        created_at,
        updated_at,
        closed_at,
        (state = 'closed')                     as is_closed,
        date(created_at)                        as created_date,
        date(closed_at)                         as closed_date,
        html_url
    from source
    where not is_pull_request
)

select * from renamed
