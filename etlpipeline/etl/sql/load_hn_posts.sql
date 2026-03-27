-- Production HN warehouse load
CREATE TABLE IF NOT EXISTS hn_posts (
    id INTEGER PRIMARY KEY,
    title TEXT,
    user TEXT,
    score INTEGER,
    comments INTEGER,
    created_at TIMESTAMP    
);

-- Clear + load from temp staging
TRUNCATE TABLE hn_posts;

-- Dynamic INSERT from XCom via Jinjia
{% for record in ti.xcom_pull(task_ids='transform_hn')['return_value'] %}
INSERT INTO hn_posts (id, title, user, score, comments, created_at)
VALUES(
    {{ record.id }}, 
    '{{ record.title }}', 
    '{{ record.user }}', 
    {{ record.score }}, 
    {{ record.comments }}, 
    '{{ record.created_at }}'
);
{% endfor %}