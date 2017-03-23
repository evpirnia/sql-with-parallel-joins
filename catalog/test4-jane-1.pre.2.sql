delete from dtables where tname='reserves';
delete from dtables where tname='sailors';
insert into dtables values ('reserves', NULL, 'jdbc:mysql://192.168.10.20:3306/testDB2', 'blakela', 'hulu', 1, 2, 'id', 5, 7);
insert into dtables values ('reserves', NULL, 'jdbc:mysql://192.168.10.10:3306/testDB1', 'evelynb', 'netflix', 1, 1, 'id', 0, 4);
insert into dtables values ('sailors', NULL, 'jdbc:mysql://192.168.10.20:3306/testDB2', 'blakela', 'hulu', 1, 2, 'boat', 3, 10);
insert into dtables values ('sailors', NULL, 'jdbc:mysql://192.168.10.10:3306/testDB1', 'evelynb', 'netflix', 1, 1, 'boat', 0, 2);
