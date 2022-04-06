# PXF Tarantool plugin

[PXF](https://gpdb.docs.pivotal.io/6-8/pxf/overview_pxf.html) plugin to write data to Tarantool cluster.

## How to deploy and use

You should have working GPDB or ADB cluster with installed PXF.

## Tarantool prerequisites

[Cartridge-java](https://github.com/tarantool/cartridge-java) look at parts: 2-5

### BUILD

```shell script
./gradlew clean build jar
```

### INSTALL

You will get a `pxf-tarantool/build` folder with target jars. Copy them to each GPDB or ADB host and delete the old one:

```shell script
cp pxf-tarantool.jar /usr/lib/pxf/lib
chown pxf:pxf /usr/lib/pxf/lib/pxf-tarantool.jar

cp msgpack-core-0.8.20.jar /usr/lib/pxf/lib/shared/
chown pxf:pxf /usr/lib/pxf/lib/shared/msgpack-core-0.9.0.jar

cp cartridge-driver-0.4.3.jar /usr/lib/pxf/lib/shared/
chown pxf:pxf /usr/lib/pxf/lib/shared/cartridge-driver-0.7.0

cp netty* /usr/lib/pxf/lib/shared/
chown pxf:pxf /usr/lib/pxf/lib/shared/netty*
```

Update profile.xml (`/var/lib/pxf/conf/pxf-profiles.xml`) by **adding** missing items from repository
one (`pxf-tarantool/env/pxf-profiles.xml`)

Then sync greenplum cluster

```shell script
pxf cluster sync
```

### CHECK

From GPDB SQL console try to execute:

```greenplum
CREATE WRITABLE EXTERNAL TABLE tarantool_tbl (a int, b varchar, bucket_id bigint)
    LOCATION ('pxf://<space>?PROFILE=<profile>&TARANTOOL_SERVER=<tarantool-router>&USER=<user>&PASSWORD=<password>')
    FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export');
-- where profile = 'tarantool-upsert' or 'tarantool-delete'

insert into tarantool_tbl
values (1, 'a', null),
       (2, 'b', null);

drop external table tarantool_tbl;
```
