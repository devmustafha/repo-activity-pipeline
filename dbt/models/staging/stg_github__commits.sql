with source as (
    select * from {{ source('github', 'commits') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['repo_full_name', 'sha']) }} as commit_key,
        repo_full_name,
        sha,
        nullif(trim(split_part(message, chr(10), 1)), '') as message_subject,
        author_name,
        lower(author_email)                            as author_email,
        author_login,
        authored_at,
        committed_at,
        coalesce(comment_count, 0)                     as comment_count,
        date(committed_at)                             as committed_date,
        html_url
    from source
)

select * from renamed
