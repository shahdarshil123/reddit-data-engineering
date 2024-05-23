CREATE TABLE Reddit (
    id VARCHAR(10),
    title TEXT,
    score INT,
    num_comments INT,
    author VARCHAR(50),
    created_utc TIMESTAMP,
    url TEXT,
    over_18 BOOLEAN,
    edited BOOLEAN,
    spoiler BOOLEAN,
    stickied BOOLEAN
);