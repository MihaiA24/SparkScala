/**
  Example table for ReadFromPostgres Scala Object
 */
create table courses
(
    id       uuid,
    name     varchar(255),
    duration varchar(255)
);

alter table courses
    owner to postgres;


insert into courses (id, name, duration) values (gen_random_uuid(), 'One', '1 day');
insert into courses (id, name, duration) values (gen_random_uuid(), 'One', '1 day');
insert into courses (id, name, duration) values (gen_random_uuid(), 'One', '1 day');
insert into courses (id, name, duration) values (gen_random_uuid(), 'Two', '2 day');
insert into courses (id, name, duration) values (gen_random_uuid(), 'Two', '2 day');
insert into courses (id, name, duration) values (gen_random_uuid(), 'Two', '2 day');