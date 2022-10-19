

create or replace procedure sp_testing.sp_test ()
begin

update aspial-bq.sp_testing.test set PK_ID =1 where PK_ID=16;

end
