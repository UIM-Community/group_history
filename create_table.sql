CREATE TABLE ssr_groups_history (
    cs_id INT not null,
	robotname varchar(255) not null,
	groupname varchar(100) not null,
	active TINYINT not null DEFAULT 1,
	created datetime DEFAULT GETDATE(),
	updated datetime DEFAULT GETDATE(),
	deleted datetime DEFAULT null
)
