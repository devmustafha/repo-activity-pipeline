with source as (
    select * from {{ source('github', 'pull_requests') }}
),

renamed as (
    select
        repo_full_name,
        number                     as pr_number,
        id                         as pr_id,
        title,
        state,
        coalesce(draft, false)     as is_draft,
        user_login                 as author_login,
        base_ref,
        head_ref,
        created_at,
        updated_at,
        closed_at,
        merged_at,
        (merged_at is not null)     as is_merged,
        date(created_at)            as created_date,
        date(merged_at)            as merged_date,
        case
            when merged_at is not null
            then extract(epoch from (merged_at - created_at)) / 86400.0
        end                        as days_to_merge
    from source
)

select * from renamed
