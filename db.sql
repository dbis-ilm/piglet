create table materializations(
  lineage char(32) primary key,
  fname varchar(255)
);

create table opcount(
  id char(32) primary key,
  cnt int default 0
);

create table exectimes(
  appname varchar(200) not null,
  stageid int not null,
  stagename varchar(100),
  lineage char(32) not null,
  stageduration bigint not null,
  progduration bigint not null,
  submissiontime bigint,
  completiontime bigint,
  size bigint
);
create index exectimes_lineage_idx on exectimes (lineage);
