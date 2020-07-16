
#create-table-oracle
BEGIN
execute immediate 'create table "AKKA_PROJECTION_OFFSET_STORE" ("PROJECTION_NAME" VARCHAR2(255) NOT NULL,"PROJECTION_KEY" VARCHAR2(255) NOT NULL,"OFFSET" VARCHAR2(255) NOT NULL,"MANIFEST" VARCHAR2(4) NOT NULL,"MERGEABLE" CHAR(1) NOT NULL check ("MERGEABLE" in (0, 1)),"LAST_UPDATED" NUMBER(19) NOT NULL) ';
execute immediate 'alter table "AKKA_PROJECTION_OFFSET_STORE" add constraint "PK_PROJECTION_ID" primary key("PROJECTION_NAME","PROJECTION_KEY") ';
execute immediate 'create index "PROJECTION_NAME_INDEX" on "AKKA_PROJECTION_OFFSET_STORE" ("PROJECTION_NAME") ';
EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE = -955 THEN
        NULL; -- suppresses ORA-00955 exception
      ELSE
         RAISE;
      END IF;
END;
#create-table-oracle