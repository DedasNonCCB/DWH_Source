CREATE OR REPLACE PACKAGE BODY DWH.PKG_OSOS_TRIAL_0010
IS
-------------------------------------------------------------------------------
--  Created By              :�akir �ilo�lu
--  Creation Date           : 2017.07.13
--  Last Modification Date  : 
--  Package Name            : PKG_OSOS_ANALYZE
--  Package Version         : 0.0.1
--  Package Description     : 
--
--  [Modification History]
-------------------------------------------------------------------------------

  -----------------------------------
  -- Initialize Log Variables      --
  -----------------------------------
  gv_job_module       constant varchar2(50)  := 'OSOS_TRIAL_0010';    -- Job Module Name
  gv_pck              constant varchar2(50)  := 'PKG_OSOS_TRIAL_0010';-- PLSQL Package Name
  gv_job_owner        constant varchar2(50)  := 'DWH';                -- Owner of the Job
  gv_proc             varchar2(100);                                  -- Procedure Name

  gv_sql_errm         varchar2(4000);                              -- SQL Error Message
  gv_sql_errc         number;                                      -- SQL Error Code
  gv_dyn_task         long := '';
  
  -- schemas
  gv_dwh_owner        constant varchar2(30) := 'DWH';
  gv_dm_owner         constant varchar2(30) := 'DM';
  gv_stg_owner        constant varchar2(30) := 'DWH';
  gv_edw_owner        constant varchar2(30) := 'DWH_EDW';
  gv_ods_owner        constant varchar2(30) := 'DWH_ODS';
  gv_ods_osos_owner   constant varchar2(30) := 'ODS_OSOS_SDM';
  gv_ods_luna_owner   constant varchar2(30) := 'ODS_OSOS_LUNA';
  gv_ods_mbs_owner    constant varchar2(30) := 'ODS_MBS_DICLE';

  -- variables
  gv_d_window_size    constant number       := 60000;
  v_daybefore         constant number       := 30;
  
  ------------------------------------------------------------------------------

  ------------------------------------------------------------------------------
  --  Created By     : �akir �ilo�lu
  --  Creation Date  : 2017.07.13
  --  Procedure Name : PRC_TMP_BULKDATALAST30
  --  Description    : NAR D�N�N READ-OUT DE�ERLER�
  -- TABLOSU
  --  [Modification History]
  --  -------------------------------------------------------------------------

  
  PROCEDURE PRC_TMP_BULKDATA0010(pid_start_date date default trunc(sysdate-31), pid_end_date date default trunc(sysdate-1))
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMP_BULKDATA0010';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(60) := 'GG_ADM_LPR_BULKDATAS';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_BULKDATA0010';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS  
    SELECT /*+ PARALLEL(16)  */
      OWNERSERNO, 
      PROFILEDATE, 
      TSUM, 
      VOLTAGE1, 
      VOLTAGE2, 
      VOLTAGE3, 
      CURRENT1, 
      CURRENT2, 
      CURRENT3, 
      COSF1, 
      COSF2, 
      COSF3, 
      RECORDSTATUS    
    FROM 
      '||gv_ods_owner||'.'||v_src_table_01||'
    WHERE 
     -- TRUNC(SYSDATE-'||v_daybefore||') = TO_DATE(SUBSTR(PROFILEDATE,1,8),''YYYYMMDD'') 
      SUBSTR(PROFILEDATE,1,8) =  '||to_char(pid_start_date,'YYYYMMDD')||' or
      SUBSTR(PROFILEDATE,1,8)  = '||to_char(pid_end_date,'YYYYMMDD')||' 
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
   ------------------------------------------------------------------------------
  --  Created By     : �akir �ilo�lu
  --  Creation Date  : 2017.07.13
  --  Procedure Name :PRC_TMP_CONSUMPTION_UNIONLAST30
  --  Description    : NAR VE LUNA SON 30 G�NL�K READ-OUT DE�ER� SAATL�K TEK�L
  -- LE�T�R�LM��
  --  [Modification History]
  --  -------------------------------------------------------------------------


PROCEDURE PRC_TMP_UNIONLAST0010(pid_start_date date default trunc(sysdate-31), pid_end_date date default trunc(sysdate-1))
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMP_CONSUMPTION_UNION0010';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'TMP_BULKDATA0010';
    v_src_table_02  varchar2(30) := 'GG_SDM_VAL_DEFINITIONITEMS';
    v_src_table_03  varchar2(30) := 'LUNA_ABONE';
    v_src_table_04  varchar2(30) := 'LUNA_SAYAC';
    v_src_table_05  varchar2(30) := 'OBISOKUMA1';
    v_src_table_06  varchar2(30) := 'OBISOKUMA2';
    v_src_table_07  varchar2(30) := 'OBISOKUMA3';
    v_src_table_08  varchar2(30) := 'OBISOKUMA4';
    v_src_table_09  varchar2(30) := 'OBISOKUMA5';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_UNIONLAST0010';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS  
    SELECT 
      SUBSCRIBER_ID,
      PROFILEDATE,
      TSUM,
      VOLTAGE1,
      VOLTAGE2,
      VOLTAGE3,
      CURRENT1, 
      CURRENT2, 
      CURRENT3,
      COSF1, 
      COSF2,
      COSF3, 
      RECORDSTATUS,
      TABLENAME,
      SR,
      ROW_DATE    
    FROM 
      (
      SELECT /*+ PARALLEL(A 16) PARALLEL(S 16) */
        S.IDENTIFIERVALUE AS SUBSCRIBER_ID,
        A.PROFILEDATE,
        A.TSUM , 
        A.VOLTAGE1, 
        A.VOLTAGE2, 
        A.VOLTAGE3, 
        A.CURRENT1, 
        A.CURRENT2, 
        A.CURRENT3,
        A.COSF1, 
        A.COSF2,
        A.COSF3, 
        TO_CHAR(A.RECORDSTATUS) AS RECORDSTATUS,
        ''NAR'' AS TABLENAME,
        ROW_NUMBER() OVER (PARTITION BY IDENTIFIERVALUE,SUBSTR(PROFILEDATE,1,10) ORDER BY PROFILEDATE) SR,
        SUBSTR(PROFILEDATE,1,8) ROW_DATE
     FROM 
        '||gv_stg_owner||'.'||v_src_table_01||'
          A LEFT JOIN '||gv_ods_owner||'.'||v_src_table_02||' S ON S.SERNO=A.OWNERSERNO 
     )
    WHERE SR=1 

    UNION ALL

    SELECT 
      SUBSCRIBER_ID,
      PROFILEDATE,
      TSUM,
      VOLTAGE1,
      VOLTAGE2,
      VOLTAGE3,
      CURRENT1, 
      CURRENT2, 
      CURRENT3,
      COSF1, 
      COSF2,
      COSF3, 
      RECORDSTATUS,
      TABLENAME,
      SR,
      ROW_DATE
    FROM 
      (
      SELECT  /*+  PARALLEL(LA 16) PARALLEL(LS 16) PARALLEL(OB1 16) PARALLEL(OB2 16) PARALLEL(OB2 16) PARALLEL(OB3 16) PARALLEL(OB4 16) PARALLEL(OB5 16)  */
        CAST(LA.ABONENO AS VARCHAR (500)) AS SUBSCRIBER_ID,
        TO_NUMBER(TO_CHAR(OB1.OKUMATARIH,''YYYYMMDDHH24MISS'' )) AS PROFILEDATE,
        TO_NUMBER(OB1.TNUMERIK) AS TSUM,
        TO_NUMBER(OB1.VOLTAJRNUMERIK) AS VOLTAGE1, 
        TO_NUMBER(OB1.VOLTAJSNUMERIK) AS VOLTAGE2, 
        TO_NUMBER(OB1.VOLTAJTNUMERIK) AS VOLTAGE3,
        TO_NUMBER(OB2.AKIMRNUMERIK) AS CURRENT1, 
        TO_NUMBER(OB2.AKIMSNUMERIK) AS CURRENT2, 
        TO_NUMBER(OB2.AKIMTNUMERIK) AS CURRENT3,
        TO_NUMBER(OB3.COSRNUMERIK) AS COSF1,
        TO_NUMBER(OB3.COSSNUMERIK) AS COSF2,
        TO_NUMBER(OB3.COSTNUMERIK) AS COSF3,
        NULL RECORDSTATUS,
        ''LUNA'' AS TABLENAME,
        ROW_NUMBER() OVER (PARTITION BY ABONENO,SUBSTR((TO_NUMBER(TO_CHAR(OB1.OKUMATARIH,''YYYYMMDDHH24MISS'' ))),1,10) ORDER BY OB1.OKUMATARIH) SR,
        SUBSTR(TO_CHAR(OKUMATARIH,''YYYYMMDDHH24MISS'' ),1,8) ROW_DATE
      FROM  
        '||gv_ods_owner||'.'||v_src_table_03||' LA
        LEFT JOIN '||gv_ods_owner||'.'||v_src_table_04||' LS ON  (LS.SAYACID=LA.SAYAC_ID)
        LEFT JOIN '||gv_ods_luna_owner||'.'||v_src_table_05||' OB1 ON (OB1.ABONE_ID = LA.ABONEID)
        LEFT JOIN '||gv_ods_luna_owner||'.'||v_src_table_06||' OB2 ON (OB1.OBISOKUMAID = OB2.OBISOKUMAID)
        LEFT JOIN '||gv_ods_luna_owner||'.'||v_src_table_07||' OB3 ON (OB1.OBISOKUMAID = OB3.OBISOKUMAID)
        LEFT JOIN '||gv_ods_luna_owner||'.'||v_src_table_08||' OB4 ON (OB1.OBISOKUMAID = OB4.OBISOKUMAID)
        LEFT JOIN '||gv_ods_luna_owner||'.'||v_src_table_09||' OB5 ON (OB1.OBISOKUMAID = OB5.OBISOKUMAID)
      WHERE  
        OB1.OKUMATIPI IN (11)  AND 
        --TRUNC(SYSDATE-'||v_daybefore||')=TRUNC(OKUMATARIH)  
        SUBSTR(TO_CHAR(OKUMATARIH,''YYYYMMDDHH24MISS'' ),1,8) = '||to_char(pid_start_date,'YYYYMMDD')||' and
        SUBSTR(TO_CHAR(OKUMATARIH,''YYYYMMDDHH24MISS'' ),1,8) = '||to_char(pid_end_date,'YYYYMMDD')||' 
      ) 
    WHERE SR=1 
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  
  PROCEDURE PRC_F_MRR_LOADPROFILES0010
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'F_MRR_LOADPROFILES0010';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'TMP_CONSUMPTION_UNION0010';
    v_src_table_02  varchar2(30) := 'D_MRR_AMR_METERS';
    v_src_table_03  varchar2(30) := 'D_SUBSCRIBERS';
    ----------------------------------------------------------------------------
   BEGIN
  
    gv_proc := 'PRC_F_MRR_LOADPROFILES0010';
  
    -- Initialize Log Variables
    plib.o_log := log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.truncate_table(gv_stg_owner, v_table_name);
    
    plib.enable_parallel_dml;
    
    gv_dyn_task := '
    INSERT /*+  APPEND NOLOGGING */ INTO '||gv_stg_owner ||'.'||v_table_name||'
    (
    SUBSCRIBER_ID,
    PROFILEDATE,
    TSUM , 
    VOLTAGE1, 
    VOLTAGE2, 
    VOLTAGE3, 
    CURRENT1, 
    CURRENT2, 
    CURRENT3,
    COSF1, 
    COSF2,
    COSF3, 
    RECORDSTATUS,
    TABLENAME,
    FLOW_MULTIPLIER,
    VOLTAGE_MULTIPLIER,
    SECTOR_ID,
    ROW_DATE
    )
    SELECT 
      /*+ PARALLEL(C 16) PARALLEL(A 16) */
      C.SUBSCRIBER_ID,
      PROFILEDATE,
      TSUM , 
      VOLTAGE1, 
      VOLTAGE2, 
      VOLTAGE3, 
      CURRENT1, 
      CURRENT2, 
      CURRENT3,
      COSF1, 
      COSF2,
      COSF3, 
      RECORDSTATUS,
      TABLENAME,
      A.FLOW_MULTIPLIER,
      A.VOLTAGE_MULTIPLIER,
      D.SUBSCRIBER_GROUP_NAME SECTOR_ID,
      C.ROW_DATE
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' C 
       LEFT JOIN '||gv_edw_owner||'.'||v_src_table_02||'  A ON  C.SUBSCRIBER_ID=A.AMR_INSTALLATION_ID 
       LEFT JOIN '||gv_edw_owner||'.'||v_src_table_03||' D ON C.SUBSCRIBER_ID=D.SUBSCRIBER_ID AND REGEXP_LIKE(C.SUBSCRIBER_ID, ''^[[:digit:]]+$'') 
     ';
     
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  
  PROCEDURE PRC_TMP_METER_MULTIPLIER0010
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMP_METER_MULTIPLIER0010';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(60) := 'SAYAC';
    v_src_table_02  varchar2(60) := 'SAYAC_DEGISIKLIK';


    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_METER_MULTIPLIER0010';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT /*+ PARALLEL (16) */
      DISTINCT
      DURUM, 
      LPAD(TESISAT_NO,8,''0'') TESISAT_NO, 
      SAYAC_KODU,
      MARKA,MODEL_KODU,
      SAYAC_NO,
      DECODE(CARPAN,0,1,NVL(CARPAN,1)) CARPAN,
      TAKILMA_TARIHI,
      TAKILMA_TARIHI BASTAR,
      LAG(TAKILMA_TARIHI,1,TRUNC(SYSDATE)) OVER(PARTITION BY  TESISAT_NO ORDER BY TAKILMA_TARIHI DESC) BITTAR,
      TO_NUMBER(TO_CHAR(TAKILMA_TARIHI,''YYYYMMDD'')) BASTARINT,
      TO_NUMBER(TO_CHAR(LAG(TAKILMA_TARIHI,1,TRUNC(SYSDATE)) OVER(PARTITION BY  TESISAT_NO ORDER BY TAKILMA_TARIHI DESC) ,''YYYYMMDD'')) BITTARINT
    FROM 
      (
      SELECT /*+ PARALLEL (16) */
        ''Guncel'' DURUM, 
        TESISAT_NO,
        SAYAC_KODU,
        MARKA,
        MODEL_KODU,
        SAYAC_NO,
        CARPAN,
        TO_DATE(TAKILMA_TARIHI,''YYYYMMDD'') TAKILMA_TARIHI 
      FROM 
        '||gv_ods_mbs_owner||'.'||v_src_table_01||'
      WHERE SAYAC_KODU = 1 
      UNION ALL
      SELECT /*+ PARALLEL (16) */
        ''Eski'' DURUM,
        TESISAT_NO,
        SAYAC_KODU,
        MARKA,
        MODEL_KODU,
        SAYAC_NO,
        CARPAN,
        TO_DATE(TAKILMA_TARIHI,''YYYYMMDD'')  
      FROM  
        '||gv_ods_mbs_owner||'.'||v_src_table_02||'
      WHERE
        SAYAC_KODU = 1 
      )           
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
PROCEDURE PRC_TMP_RAWDATA0010
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMP_RAWDATA0010';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(60) := 'F_MRR_LOADPROFILES0010';
    v_src_table_02  varchar2(60) := 'TMP_METER_MULTIPLIER0010';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_RAWDATA0010';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT /*+PARALLEL (L,16) PARALLEL (SD,4) PARALLEL (W,4) */
      CASE 
          WHEN SD.CARPAN IS NULL THEN CASE WHEN L.VOLTAGE_MULTIPLIER > 1 THEN ''P'' ELSE ''S'' END
          ELSE CASE WHEN (SD.CARPAN  < 301) OR (SD.CARPAN  IN (400, 320)) THEN ''S'' ELSE ''P'' END END ABONETYPE,
      NVL(L.FLOW_MULTIPLIER,0) AS FLOWMULTIPLIER,
      NVL(L.VOLTAGE_MULTIPLIER,0)  AS  VOLTAGEMULTIPLIER,
      SD.DURUM, 
      SD.MARKA,
      SD.CARPAN,
      SD.SAYAC_NO, 
      SD.TAKILMA_TARIHI, 
      L.SUBSCRIBER_ID,
      L.PROFILEDATE, 
      L.TSUM, 
      L.VOLTAGE1, 
      L.VOLTAGE2, 
      L.VOLTAGE3, 
      L.CURRENT1, 
      L.CURRENT2, 
      L.CURRENT3, 
      L.COSF1, 
      L.COSF2, 
      L.COSF3, 
      L.RECORDSTATUS, 
      L.TABLENAME,
      L.SECTOR_ID,
      L.ROW_DATE
    FROM  
      '||gv_stg_owner||'.'||v_src_table_01||' L
      LEFT JOIN '||gv_stg_owner||'.'||v_src_table_02||' SD ON (SD.TESISAT_NO = L.SUBSCRIBER_ID 
      AND TO_NUMBER(SUBSTR(PROFILEDATE,1,8)) BETWEEN SD.BASTARINT AND SD.BITTARINT)
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  PROCEDURE PRC_TMP_RAWDATA0010_C
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMP_RAWDATA0010_C';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(60) := 'TMP_RAWDATA0010';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_RAWDATA0010_C';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT /*+ PARALLEL (32) */
      T.ABONETYPE,
      T.TSUM ENDEX,
      LEAD(TSUM,1) OVER(PARTITION BY T.SUBSCRIBER_ID,T.SAYAC_NO ORDER BY T.PROFILEDATE ASC) NEXTENDEX,
      LEAD(TSUM,2) OVER(PARTITION BY T.SUBSCRIBER_ID,T.SAYAC_NO ORDER BY T.PROFILEDATE ASC) NEXTNEXTENDEX,
      T.VOLTAGE1 V1,
      LEAD(T.VOLTAGE1,1) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) V1NEXT,
      LEAD(T.VOLTAGE1,2) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) V1NEXTNEXT,
      T.VOLTAGE2 V2,
      LEAD(T.VOLTAGE2,1) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) V2NEXT,
      LEAD(T.VOLTAGE2,2) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) V2NEXTNEXT,
      T.VOLTAGE3 V3,
      LEAD(T.VOLTAGE3,1) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) V3NEXT,
      LEAD(T.VOLTAGE3,2) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) V3NEXTNEXT,
      T.CURRENT1 I1,
      LEAD(T.CURRENT1,1) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) I1NEXT,
      LEAD(T.CURRENT1,2) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) I1NEXTNEXT,
      T.CURRENT2 I2,
      LEAD(T.CURRENT2,1) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) I2NEXT,
      LEAD(T.CURRENT2,2) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) I2NEXTNEXT,
      T.CURRENT3 I3,
      LEAD(T.CURRENT3,1) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) I3NEXT,
      LEAD(T.CURRENT3,2) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) I3NEXTNEXT,
      T.COSF1 COS1,
      LEAD(T.COSF1,1) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) C1NEXT,
      LEAD(T.COSF1,2) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) C1NEXTNEXT,
      T.COSF2 COS2,
      LEAD(T.COSF2,1) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) C2NEXT,
      LEAD(T.COSF2,2) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) C2NEXTNEXT,
      T.COSF3 COS3,
      LEAD(T.COSF3,1) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) C3NEXT,
      LEAD(T.COSF3,2) OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC) C3NEXTNEXT,
      T.SUBSCRIBER_ID WIRINGNO,
      T.VOLTAGEMULTIPLIER,
      T.FLOWMULTIPLIER,
      T.PROFILEDATE,   
      --ROW_NUMBER() OVER(PARTITION BY T.SUBSCRIBER_ID ORDER BY T.PROFILEDATE ASC ) RN,
      t.CARPAN MBS_MULTIPLIER,
      t.SECTOR_ID SECTORINFO,
      T.SAYAC_NO,
      ROW_DATE 
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' T    
      ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  
  PROCEDURE PRC_TMP_VOLTAGE_PRIMER0010
  IS
    ----------------------------------------------------------------------------
    v_table_name            varchar2(35) := 'VOLTAGE_P_TAG001';
    v_src_table_01          varchar2(60) := 'TMP_RAWDATA0010_C';
    v_src_table_02          varchar2(60) := 'D_DEFINITIONS';
    v_src_table_03          varchar2(60) := 'D_GROUPS';
    V_PABONE_TYPE           CHAR(1) :=      NULL;
    V_PVOLTAGE_MIN          VARCHAR2(10) := NULL;
    V_PVOLTAGE_MAX          VARCHAR2(10):=  NULL;
    V_PANALYZE_MONTH_RANGE  VARCHAR2(10) := NULL;
    V_PSTEP_CLOCK           VARCHAR2(10):=  NULL;
    V_PSCORE                VARCHAR2(10) := NULL;
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_VOLTAGE_PRIMER0010';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
       

    plib.drop_table(gv_stg_owner, v_table_name);
    
    GV_DYN_TASK := '
    SELECT 
      MAX(CASE WHEN CODE= ''ABONE_TYPE'' THEN PARAMETER_1 END) ABONE_TYPE,
      MAX(CASE WHEN CODE= ''VOLTAGE_MIN'' THEN PARAMETER_1 END) VOLTAGE_MIN,
      MAX(CASE WHEN CODE= ''VOLTAGE_MAX'' THEN PARAMETER_1 END) VOLTAGE_MAX,
      MAX(CASE WHEN CODE= ''ANALYZE_MONTH_RANGE'' THEN PARAMETER_1 END) ANALYZE_MONTH_RANGE,
      MAX(CASE WHEN CODE= ''STEP_CLOCK'' THEN PARAMETER_1 END)-1 STEP_CLOCK,
      MAX(CASE WHEN CODE= ''SCORE'' THEN PARAMETER_1 END) SCORE 
    INTO 
      V_PABONE_TYPE,
      V_PVOLTAGE_MIN,
      V_PVOLTAGE_MAX,
      V_PANALYZE_MONTH_RANGE,
      V_PSTEP_CLOCK,V_PSCORE
    FROM   '||gv_edw_owner||'.'||v_src_table_02||' D 
      JOIN '||gv_edw_owner||'.'||v_src_table_03||' G ON (D.RID = G.MASTER_ID) 
    WHERE 
      D.D_CODE =''subscribe_Analyze''
      AND D.D_DESCRIPTION =''GERILIM_DENGESIZLIGI_Primer''
    '
    execute immediate gv_dyn_task;  
          
    GV_DYN_TASK := '
    CREATE TABLE '||GV_STG_OWNER||'.'||V_TABLE_NAME||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT /*+PARALLEL (L,16) */ 
      WIRINGNO,
      PROFILEDATE,
      V1,
      V2,
      V3,
      I1,
      I2,
      I3,
      COS1,
      COS2,
      COS3,
      FLOWMULTIPLIER,
      VOLTAGEMULTIPLIER,
      ABONETYPE,
      CASE 
        WHEN (
              (V1 > '||V_PVOLTAGE_MAX||' AND V1NEXT > '||V_PVOLTAGE_MAX||' AND V1NEXTNEXT > '||V_PVOLTAGE_MAX||') OR
              (V2 > '||V_PVOLTAGE_MAX||' AND V2NEXT > '||V_PVOLTAGE_MAX||' AND V2NEXTNEXT > '||V_PVOLTAGE_MAX||') OR
              (V3 > '||V_PVOLTAGE_MAX||' AND V3NEXT > '||V_PVOLTAGE_MAX||' AND V3NEXTNEXT > '||V_PVOLTAGE_MAX||')
             ) THEN ''B''
        WHEN (
             (V1 <= '||V_PVOLTAGE_MIN||' AND V1NEXT <= '||V_PVOLTAGE_MIN||' AND V1NEXTNEXT <= '||V_PVOLTAGE_MIN||') OR 
             (V2 <=  '||V_PVOLTAGE_MIN||' AND V2NEXT<= '||V_PVOLTAGE_MIN||' AND V2NEXTNEXT <= '||V_PVOLTAGE_MIN||') OR 
             (V3 <=  '||V_PVOLTAGE_MIN||' AND V3NEXT<= '||V_PVOLTAGE_MIN||' AND V3NEXTNEXT <= '||V_PVOLTAGE_MIN||')
             ) THEN ''D'' END FAULT_TYPE,
      '||V_PSCORE||' AS SCORE,
      MBS_MULTIPLIER,
      SAYAC_NO, 
      ROW_DATE
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' L 
    WHERE  L.ABONETYPE = ''P''
      AND (
           (
             (V1 > '||V_PVOLTAGE_MAX||' AND V1NEXT > '||V_PVOLTAGE_MAX||' AND V1NEXTNEXT > '||V_PVOLTAGE_MAX||') OR
             (V2 > '||V_PVOLTAGE_MAX||' AND V2NEXT > '||V_PVOLTAGE_MAX||' AND V2NEXTNEXT > '||V_PVOLTAGE_MAX||') OR
             (V3 > '||V_PVOLTAGE_MAX||' AND V3NEXT > '||V_PVOLTAGE_MAX||' AND V3NEXTNEXT > '||V_PVOLTAGE_MAX||')
           )       
           OR  
           (     
             (V1 <= '||V_PVOLTAGE_MIN||'  AND V1NEXT<= '||V_PVOLTAGE_MIN||' AND V1NEXTNEXT <= '||V_PVOLTAGE_MIN||') OR 
             (V2 <=  '||V_PVOLTAGE_MIN||' AND V2NEXT<= '||V_PVOLTAGE_MIN||' AND V2NEXTNEXT <= '||V_PVOLTAGE_MIN||') OR 
             (V3 <=  '||V_PVOLTAGE_MIN||' AND V3NEXT<= '||V_PVOLTAGE_MIN||' AND V3NEXTNEXT <= '||V_PVOLTAGE_MIN||')
           )            
         ) 
          
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  PROCEDURE PRC_TMP_VOLTAGE_SECONDER0010
  IS
    ----------------------------------------------------------------------------
    v_table_name            varchar2(35) := 'VOLTAGE_S_TAG001';
    v_src_table_01          varchar2(60) := 'TMP_RAWDATA0010_C';
    v_src_table_02          varchar2(60) := 'D_DEFINITIONS';
    v_src_table_03          varchar2(60) := 'D_GROUPS';
    V_SABONE_TYPE           CHAR(1):=       NULL;
    V_SVOLTAGE_MIN          VARCHAR2(10):=  NULL;
    V_SVOLTAGE_MAX          VARCHAR2(10):=  NULL;
    V_SANALYZE_MONTH_RANGE  VARCHAR2(10):=  NULL;
    V_SSTEP_CLOCK           VARCHAR2(10):=  NULL;
    V_SSCORE                VARCHAR2(10):=  NULL;
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_VOLTAGE_SECONDER0010';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
       

    plib.drop_table(gv_stg_owner, v_table_name);

    GV_DYN_TASK := '         
    SELECT 
      MAX(CASE WHEN CODE= ''ABONE_TYPE'' THEN PARAMETER_1 END) ABONE_TYPE,
      MAX(CASE WHEN CODE= ''VOLTAGE_MIN'' THEN PARAMETER_1 END) VOLTAGE_MIN,
      MAX(CASE WHEN CODE= ''VOLTAGE_MAX'' THEN PARAMETER_1 END) VOLTAGE_MAX,
      MAX(CASE WHEN CODE= ''ANALYZE_MONTH_RANGE'' THEN PARAMETER_1 END) ANALYZE_MONTH_RANGE,
      MAX(CASE WHEN CODE= ''STEP_CLOCK'' THEN PARAMETER_1 END)-1 STEP_CLOCK,
      MAX(CASE WHEN CODE= ''SCORE'' THEN PARAMETER_1 END) SCORE 
    INTO 
      V_SABONE_TYPE,
      V_SVOLTAGE_MIN,
      V_SVOLTAGE_MAX,
      V_SANALYZE_MONTH_RANGE,
      V_SSTEP_CLOCK,
      V_SSCORE           
    FROM   '||gv_edw_owner||'.'||v_src_table_02||' D 
      JOIN '||gv_edw_owner||'.'||v_src_table_03||' G ON (D.RID = G.MASTER_ID) 
    WHERE 
      D.D_CODE =''subscribe_Analyze''
      AND D.D_DESCRIPTION =''GERILIM_DENGESIZLIGI_Sekonder''
    ';

    execute immediate gv_dyn_task;

    GV_DYN_TASK := '
    CREATE TABLE '||GV_STG_OWNER||'.'||V_TABLE_NAME||' 
    PARALLEL NOLOGGING COMPRESS
    AS             
    SELECT /*+PARALLEL (L,16) */  
      WIRINGNO,
      PROFILEDATE,
      V1,
      V2,
      V3,
      I1,
      I2,
      I3,
      COS1,
      COS2,
      COS3,
      FLOWMULTIPLIER,
      VOLTAGEMULTIPLIER,
      ABONETYPE,
      CASE 
        WHEN (
             (V1 <= '||V_SVOLTAGE_MIN||' AND V1NEXT<= '||V_SVOLTAGE_MIN||' AND V1NEXTNEXT<= '||V_SVOLTAGE_MIN||') OR
             (V2 <= '||V_SVOLTAGE_MIN||' AND V2NEXT<= '||V_SVOLTAGE_MIN||' AND V2NEXTNEXT<= '||V_SVOLTAGE_MIN||') OR 
             (V3 <= '||V_SVOLTAGE_MIN||' AND V3NEXT<= '||V_SVOLTAGE_MIN||' AND V3NEXTNEXT<= '||V_SVOLTAGE_MIN||')
             ) THEN ''D''
        WHEN (
             (V1 >= '||V_SVOLTAGE_MAX||' AND V1NEXT>= '||V_SVOLTAGE_MAX||' AND V1NEXTNEXT<= '||V_SVOLTAGE_MAX||') OR 
             (V2 >= '||V_SVOLTAGE_MAX||' AND V2NEXT>= '||V_SVOLTAGE_MAX||' AND V2NEXTNEXT<= '||V_SVOLTAGE_MAX||') OR
             (V3 >= '||V_SVOLTAGE_MAX||' AND V3NEXT>= '||V_SVOLTAGE_MAX||' AND V3NEXTNEXT<= '||V_SVOLTAGE_MAX||')
             ) THEN ''B''  END FAULT_TYPE,
      '||V_SSCORE||' AS SCORE,
      --RN,
      MBS_MULTIPLIER,
      SAYAC_NO,
      ROW_DATE
    FROM 
    '||gv_stg_owner||'.'||v_src_table_01||' 
    WHERE  
      ABONETYPE = ''S''
      AND (
            (
             (V1 <= '||V_SVOLTAGE_MIN||' AND V1NEXT<= '||V_SVOLTAGE_MIN||' AND V1NEXTNEXT<= '||V_SVOLTAGE_MIN||') OR
             (V2 <= '||V_SVOLTAGE_MIN||' AND V2NEXT<= '||V_SVOLTAGE_MIN||' AND V2NEXTNEXT<= '||V_SVOLTAGE_MIN||') OR
             (V3 <= '||V_SVOLTAGE_MIN||' AND V3NEXT<= '||V_SVOLTAGE_MIN||' AND V3NEXTNEXT<= '||V_SVOLTAGE_MIN||')
            )
            OR 
            (
              (V1 >= '||V_SVOLTAGE_MAX||' AND V1NEXT>= '||V_SVOLTAGE_MAX||' AND V1NEXTNEXT>= '||V_SVOLTAGE_MAX||') OR 
              (V2 >= '||V_SVOLTAGE_MAX||' AND V2NEXT>= '||V_SVOLTAGE_MAX||' AND V2NEXTNEXT>= '||V_SVOLTAGE_MAX||') OR 
              (V3 >= '||V_SVOLTAGE_MAX||' AND V3NEXT>= '||V_SVOLTAGE_MAX||' AND V3NEXTNEXT>= '||V_SVOLTAGE_MAX||')
            )
          )
     ';
     

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;  
  
  
  PROCEDURE PRC_TMP_VOLTAGE_ERROR0010
  IS
    ----------------------------------------------------------------------------
    v_table_name            varchar2(35) := 'VOLTAGE_ERROR_TAG001';
    
    v_src_table_01          varchar2(60) := 'VOLTAGE_P_TAG001';
    v_src_table_02          varchar2(60) := 'VOLTAGE_S_TAG001';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_VOLTAGE_ERROR0010';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
       

    plib.drop_table(gv_stg_owner, v_table_name);
    GV_DYN_TASK := '
    CREATE TABLE '||GV_STG_OWNER||'.'||V_TABLE_NAME||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT
      WIRINGNO, 
      PROFILEDATE, 
      V1, 
      V2, 
      V3, 
      I1, 
      I2, 
      I3, 
      COS1, 
      COS2, 
      COS3, 
      FLOWMULTIPLIER, 
      VOLTAGEMULTIPLIER, 
      ABONETYPE, 
      FAULT_TYPE, 
      SCORE, 
      --RN, 
      MBS_MULTIPLIER, 
      SAYAC_NO,
      TAGTYPE,
      ROW_NUM,
      ROW_DATE
     FROM
      (
      SELECT
        WIRINGNO, 
        PROFILEDATE, 
        V1, 
        V2, 
        V3, 
        I1, 
        I2, 
        I3, 
        COS1, 
        COS2, 
        COS3, 
        FLOWMULTIPLIER, 
        VOLTAGEMULTIPLIER, 
        ABONETYPE, 
        FAULT_TYPE, 
        SCORE, 
        --RN, 
        MBS_MULTIPLIER,
        SAYAC_NO, 
        1 AS TAGTYPE,
        ROW_NUMBER() OVER(PARTITION BY WIRINGNO,ROW_DATE,FAULT_TYPE ORDER BY PROFILEDATE DESC ) ROW_NUM,
        ROW_DATE
     FROM 
      '||gv_stg_owner||'.'||v_src_table_01||'  WHERE FAULT_TYPE IS NOT NULL
      ) WHERE ROW_NUM=1
      
    UNION ALL
      
    SELECT
      WIRINGNO, 
      PROFILEDATE, 
      V1, 
      V2, 
      V3, 
      I1, 
      I2, 
      I3, 
      COS1, 
      COS2, 
      COS3, 
      FLOWMULTIPLIER, 
      VOLTAGEMULTIPLIER, 
      ABONETYPE, 
      FAULT_TYPE, 
      SCORE, 
      --RN, 
      MBS_MULTIPLIER, 
      SAYAC_NO, 
      TAGTYPE,
      ROW_NUM,
      ROW_DATE
     FROM
      (
      SELECT
        WIRINGNO, 
        PROFILEDATE, 
        V1, 
        V2, 
        V3, 
        I1, 
        I2, 
        I3, 
        COS1, 
        COS2, 
        COS3, 
        FLOWMULTIPLIER, 
        VOLTAGEMULTIPLIER, 
        ABONETYPE, 
        FAULT_TYPE, 
        SCORE, 
       -- RN, 
        MBS_MULTIPLIER, 
        SAYAC_NO,
        1 AS TAGTYPE,
        ROW_NUMBER() OVER ( PARTITION BY WIRINGNO,ROW_DATE,FAULT_TYPE ORDER BY PROFILEDATE DESC ) ROW_NUM,
        ROW_DATE
    FROM 
      '||gv_stg_owner||'.'||v_src_table_02||'  WHERE FAULT_TYPE IS NOT NULL
      )
      WHERE ROW_NUM=1
     ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
   PROCEDURE PRC_TMP_FLOW_PS0020
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'FLOW_PS_TAG002';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(60) := 'TMP_RAWDATA0010_C';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_FLOW_PS0020';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 

    SELECT 
      WIRINGNO,
      PROFILEDATE,
      V1,
      V2,
      V3,
      I1,
      I2,
      I3,
      COS1,
      COS2,
      COS3,
      FLOWMULTIPLIER,
      VOLTAGEMULTIPLIER,
      ABONETYPE,
      MBS_MULTIPLIER,
      SAYAC_NO ,
      ROW_DATE
       
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' L 
    WHERE 
      NVL(L.SECTORINFO,'''') NOT LIKE ''%TAR%'' AND  
      (
      (I1>0.1 AND I1NEXT>0.1 AND I1NEXTNEXT>0.1) AND  (I2=0   AND I2NEXT=0   AND I2NEXTNEXT=0)   AND  (I3=0 AND I3NEXT=0 AND I3NEXTNEXT=0) OR 
      (I2>0.1 AND I2NEXT>0.1 AND I2NEXTNEXT>0.1) AND  (I1=0   AND I1NEXT=0   AND I1NEXTNEXT=0)   AND  (I3=0 AND I3NEXT=0 AND I3NEXTNEXT=0) OR 
      (I3>0.1 AND I3NEXT>0.1 AND I3NEXTNEXT>0.1) AND  (I1=0   AND I1NEXT=0   AND I1NEXTNEXT=0)   AND  (I2=0 AND I2NEXT=0 AND I2NEXTNEXT=0) OR 
      (I1>0.1 AND I1NEXT>0.1 AND I1NEXTNEXT>0.1) AND  (I2>0.1 AND I2NEXT>0.1 AND I2NEXTNEXT=0)   AND  (I3=0 AND I3NEXT=0 AND I3NEXTNEXT=0) OR 
      (I2>0.1 AND I2NEXT>0.1 AND I2NEXTNEXT>0.1) AND  (I3>0.1 AND I3NEXT>0.1 AND I3NEXTNEXT=0)   AND  (I2=0 AND I2NEXT=0 AND I2NEXTNEXT=0) OR 
      (I3>0.1 AND I3NEXT>0.1 AND I3NEXTNEXT>0.1) AND  (I1>0.1 AND I1NEXT>0.1 AND I1NEXTNEXT=0)   AND  (I1=0 AND I1NEXT=0 AND I1NEXTNEXT=0)
      )
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  
  
  
  
   PROCEDURE PRC_TMP_FLOW_TR0020
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'FLOW_TR_TAG002';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(60) := 'TMP_RAWDATA0010_C';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_FLOW_TR0020';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT 
      WIRINGNO,
      PROFILEDATE,
      V1,
      V2,
      V3,
      I1,
      I2,
      I3,
      COS1,
      COS2,
      COS3,
      FLOWMULTIPLIER,
      VOLTAGEMULTIPLIER,
      ABONETYPE,
      MBS_MULTIPLIER,
      SAYAC_NO,
      ROW_DATE
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' L
    WHERE 
      NVL(L.SECTORINFO,'''') LIKE ''%TAR%'' AND 
      (
        ((L.FLOWMULTIPLIER*I1 >=40 AND L.FLOWMULTIPLIER*I1NEXT>=40 AND L.FLOWMULTIPLIER*I1NEXTNEXT>=40 ) AND ((I2=0 AND I2NEXT =0  AND I2NEXTNEXT =0)  OR (I3=0 AND I3NEXT =0  AND I3NEXTNEXT =0)) ) OR 
        ((L.FLOWMULTIPLIER*I2 >=40 AND L.FLOWMULTIPLIER*I2NEXT>=40 AND L.FLOWMULTIPLIER*I2NEXTNEXT>=40 ) AND ((I1=0 AND I1NEXT =0  AND I1NEXTNEXT =0 ) OR (I3=0 AND I3NEXT =0  AND I3NEXTNEXT =0)) ) OR 
        ((L.FLOWMULTIPLIER*I3 >=40 AND L.FLOWMULTIPLIER*I3NEXT>=40 AND L.FLOWMULTIPLIER*I3NEXTNEXT>=40 ) AND ((I1=0 AND I1NEXT =0  AND I1NEXTNEXT =0 ) OR (I2=0 AND I2NEXT =0  AND I2NEXTNEXT =0)) ) OR  
        (
            (I1>0 AND I1NEXT>0 AND I1NEXTNEXT>0 AND I2>0 AND I2NEXT>0 AND I2NEXTNEXT>0 AND I3>0 AND I3NEXT>0 AND I3NEXTNEXT>0) 
            AND 
            (
               (CASE--en b�y�k
                  WHEN I1 >= I2 AND I1NEXT >= I2NEXT AND I1NEXTNEXT >= I2NEXTNEXT AND I1 >= I3 AND I1NEXT >= I3NEXT AND I1NEXTNEXT >= I3NEXTNEXT THEN (L.FLOWMULTIPLIER)*I1
                  WHEN I2 >= I1 AND I2NEXT >= I1NEXT AND I2NEXTNEXT >= I1NEXTNEXT AND I2 >= I3 AND I2NEXT >= I3NEXT AND I2NEXTNEXT >= I3NEXTNEXT THEN (L.FLOWMULTIPLIER)*I2
                  WHEN I3 >= I1 AND I3NEXT >= I1NEXT AND I3NEXTNEXT >= I1NEXTNEXT AND I3 >= I2 AND I3NEXT >= I2NEXT AND I3NEXTNEXT >= I2NEXTNEXT THEN (L.FLOWMULTIPLIER)*I3
                END -
                CASE--en k���k
                  WHEN I1 <= I2 AND I1NEXT <= I2NEXT AND I1NEXTNEXT <= I2NEXTNEXT AND I1 <= I3 AND I1NEXT <= I3NEXT AND I1NEXTNEXT <= I3NEXTNEXT THEN (L.FLOWMULTIPLIER)*I1
                  WHEN I2 <= I1 AND I2NEXT <= I1NEXT AND I2NEXTNEXT <= I1NEXTNEXT AND I2 <= I3 AND I2NEXT <= I3NEXT AND I2NEXTNEXT <= I3NEXTNEXT THEN (L.FLOWMULTIPLIER)*I2
                  WHEN I3 <= I1 AND I3NEXT <= I1NEXT AND I3NEXTNEXT <= I1NEXTNEXT AND I3 <= I2 AND I3NEXT <= I2NEXT AND I3NEXTNEXT <= I3NEXTNEXT THEN (L.FLOWMULTIPLIER)*I3
                END
               )>
               (CASE--en k���k
                WHEN I1 <= I2 AND I1NEXT <= I2NEXT AND I1NEXTNEXT <= I2NEXTNEXT AND I1 <= I3 AND I1NEXT <= I3NEXT AND I1NEXTNEXT <= I3NEXTNEXT THEN (L.FLOWMULTIPLIER)*I1
                WHEN I2 <= I1 AND I2NEXT <= I1NEXT AND I2NEXTNEXT <= I1NEXTNEXT AND I2 <= I3 AND I2NEXT <= I3NEXT AND I2NEXTNEXT <= I3NEXTNEXT THEN (L.FLOWMULTIPLIER)*I2
                WHEN I3 <= I1 AND I3NEXT <= I1NEXT AND I3NEXTNEXT <= I1NEXTNEXT AND I3 <= I2 AND I3NEXT <= I2NEXT AND I3NEXTNEXT <= I2NEXTNEXT THEN (L.FLOWMULTIPLIER)*I3
               END /2
               )
            )
            AND 
            (CASE--en b�y�k
              WHEN I1 >= I2 AND I1NEXT >= I2NEXT AND I1NEXTNEXT >= I2NEXTNEXT AND I1 >= I3 AND I1NEXT >= I3NEXT AND I1NEXTNEXT >= I3NEXTNEXT THEN (L.FLOWMULTIPLIER)*I1
              WHEN I2 >= I1 AND I2NEXT >= I1NEXT AND I2NEXTNEXT >= I1NEXTNEXT AND I2 >= I3 AND I2NEXT >= I3NEXT AND I2NEXTNEXT >= I3NEXTNEXT THEN (L.FLOWMULTIPLIER)*I2
              WHEN I3 >= I1 AND I3NEXT >= I1NEXT AND I3NEXTNEXT >= I1NEXTNEXT AND I3 >= I2 AND I3NEXT >= I2NEXT AND I3NEXTNEXT >= I2NEXTNEXT THEN (L.FLOWMULTIPLIER)*I3
            END >100
            )
        )
      )
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  PROCEDURE PRC_TMP_FLOW_TAG0020
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'FLOW_TAG002';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(60) := 'FLOW_PS_TAG002';
    v_src_table_02  varchar2(60) := 'FLOW_TR_TAG002';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_FLOW_TAG0020';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT
      WIRINGNO,
      PROFILEDATE,
      V1,
      V2,
      V3,
      I1,
      I2,
      I3,
      COS1,
      COS2,
      COS3,
      FLOWMULTIPLIER,
      VOLTAGEMULTIPLIER,
      ABONETYPE,
      SCORE,
      TAGTYPE,
      ROW_DATE
    FROM
      (
      SELECT
        WIRINGNO,
        PROFILEDATE,
        V1,
        V2,
        V3,
        I1,
        I2,
        I3,
        COS1,
        COS2,
        COS3,
        FLOWMULTIPLIER,
        VOLTAGEMULTIPLIER,
        ABONETYPE,
        1 AS SCORE,
        2 AS TAGTYPE,
        ROW_DATE,
        ROW_NUMBER() OVER ( PARTITION BY WIRINGNO,ROW_DATE ORDER BY PROFILEDATE DESC ) ROW_NUM   
      FROM 
        '||gv_stg_owner||'.'||v_src_table_01||'
        )
      WHERE 
        ROW_NUM =1
        
      UNION ALL
      
      SELECT
       WIRINGNO,
        PROFILEDATE,
        V1,
        V2,
        V3,
        I1,
        I2,
        I3,
        COS1,
        COS2,
        COS3,
        FLOWMULTIPLIER,
        VOLTAGEMULTIPLIER,
        ABONETYPE,
        SCORE,
        TAGTYPE,
        ROW_DATE
      FROM
      (
      SELECT
        WIRINGNO,
        PROFILEDATE,
        V1,
        V2,
        V3,
        I1,
        I2,
        I3,
        COS1,
        COS2,
        COS3,
        FLOWMULTIPLIER,
        VOLTAGEMULTIPLIER,
        ABONETYPE,
        5 AS SCORE,
        2 AS TAGTYPE,
        ROW_DATE,
        ROW_NUMBER() OVER ( PARTITION BY WIRINGNO,ROW_DATE ORDER BY PROFILEDATE DESC ) ROW_NUM
      FROM 
        '||gv_stg_owner||'.'||v_src_table_02||'
        )
      WHERE 
        ROW_NUM =1
        ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  PROCEDURE PRC_TMP_COS_P0030
  IS
    ----------------------------------------------------------------------------
    v_table_name            varchar2(35) := 'COS_P_TAG003';
    ----------------------------------------------------------------------------
    v_src_table_01          varchar2(60) := 'TMP_RAWDATA0010_C';
    v_src_table_02          varchar2(60) := 'D_DEFINITIONS';
    v_src_table_03          varchar2(60) := 'D_GROUPS';
    V_PABONE_TYPE           CHAR(1)      :=      NULL;
    V_PFLOW_MIN             VARCHAR2(10) :=      NULL;
    V_PCOS_MIN              VARCHAR2(10) :=      NULL;
    V_PCOS_MAX              VARCHAR2(10) :=      NULL;
    V_PANALYZE_MONTH_RANGE  VARCHAR2(10) :=      NULL;
    V_PSTEP_CLOCK           VARCHAR2(10) :=      NULL;
    V_PSCORE                VARCHAR2(10) :=      NULL;
    ----------------------------------------------------------------------------
BEGIN
    gv_proc := 'PRC_TMP_COS_P0030';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
       

    plib.drop_table(gv_stg_owner, v_table_name);

    gv_dyn_task := '
    SELECT 
      max(case when CODE= ''ABONE_TYPE'' then PARAMETER_1 end) ABONE_TYPE,
      max(case when CODE= ''FLOW_MIN'' then PARAMETER_1 end) FLOW_MIN,
      max(case when CODE= ''COS_MIN'' then PARAMETER_1 end) COS_MIN,
      max(case when CODE= ''COS_MAX'' then PARAMETER_1 end) COS_MAX,            
      max(case when CODE= ''ANALYZE_MONTH_RANGE'' then PARAMETER_1 end) ANALYZE_MONTH_RANGE,
      max(case when CODE= ''STEP_CLOCK'' then PARAMETER_1 end)-1 STEP_CLOCK,
      max(case when CODE= ''SCORE'' then PARAMETER_1 end) SCORE 
    INTO  
      V_PABONE_TYPE,
      V_PFLOW_MIN,
      V_PCOS_MIN,
      V_PCOS_MAX,
      V_PANALYZE_MONTH_RANGE,
      V_PSTEP_CLOCK,
      V_PSCORE
    FROM   '||gv_edw_owner||'.'||v_src_table_02||' D 
      JOIN '||gv_edw_owner||'.'||v_src_table_03||' G ON (D.RID = G.MASTER_ID) 
    WHERE 
      D.D_CODE =''subscribe_Analyze''
      AND D.D_DESCRIPTION =''COS_DEGERI_DUSUK_OLAN_Primer''
    ';            

    execute immediate gv_dyn_task;

    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS   
    SELECT  /*+ PARALLEL(L,16) */ 
      WIRINGNO,
      PROFILEDATE,
      V1,
      V2,
      V3,
      I1,
      I2,
      I3,
      COS1,
      COS2,
      COS3,
      FLOWMULTIPLIER,
      VOLTAGEMULTIPLIER,
      ABONETYPE,
      MBS_MULTIPLIER,
      SAYAC_NO,
      SUBSTR(PROFILEDATE,1,8) ROW_DATE
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' L  
    WHERE 
      NVL(L.SECTORINFO,'''') NOT LIKE ''%TAR%''
      AND L.ABONETYPE =''P''
      AND 
      (
          ( 
            (COS1 BETWEEN '||V_PCOS_MIN||' AND '||V_PCOS_MAX||' AND C1NEXT BETWEEN '||V_PCOS_MIN||' AND '||V_PCOS_MAX||' AND C1NEXTNEXT BETWEEN '||V_PCOS_MIN||' AND '||V_PCOS_MAX||')  
            OR 
            (COS2 BETWEEN '||V_PCOS_MIN||' AND '||V_PCOS_MAX||' AND C2NEXT BETWEEN '||V_PCOS_MIN||' AND '||V_PCOS_MAX||' AND C2NEXTNEXT BETWEEN '||V_PCOS_MIN||' AND '||V_PCOS_MAX||' ) 
            OR 
            (COS3 BETWEEN '||V_PCOS_MIN||' AND '||V_PCOS_MAX||' AND C3NEXT BETWEEN '||V_PCOS_MIN||' AND '||V_PCOS_MAX||' AND C3NEXTNEXT BETWEEN '||V_PCOS_MIN||' AND '||V_PCOS_MAX||' ) 
          )
          AND (I1> '||V_PFLOW_MIN||' AND I2> '||V_PFLOW_MIN||' AND I3> '||V_PFLOW_MIN||')
      )
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  PROCEDURE PRC_TMP_COS_S0030
  IS
    ----------------------------------------------------------------------------
    v_table_name            varchar2(35) := 'COS_S_TAG003';
    ----------------------------------------------------------------------------
    v_src_table_01          varchar2(60) := 'TMP_RAWDATA0010_C';
    v_src_table_02          varchar2(60) := 'D_DEFINITIONS';
    v_src_table_03          varchar2(60) := 'D_GROUPS';
    V_SABONE_TYPE           CHAR(1)      :=      NULL; 
    V_SFLOW_MIN             VARCHAR2(10) :=      NULL;
    V_SCOS_MIN              VARCHAR2(10) :=      NULL;
    V_SCOS_MAX              VARCHAR2(10) :=      NULL;
    V_SANALYZE_MONTH_RANGE  VARCHAR2(10) :=      NULL;
    V_SSTEP_CLOCK           VARCHAR2(10) :=      NULL;
    V_SSCORE                VARCHAR2(10) :=      NULL;

    ----------------------------------------------------------------------------
BEGIN
    gv_proc := 'PRC_TMP_COS_S0030';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
       

    plib.drop_table(gv_stg_owner, v_table_name);

    gv_dyn_task := '     
      SELECT 
        max(case when CODE= ''ABONE_TYPE'' then PARAMETER_1 end) ABONE_TYPE,
        max(case when CODE= ''FLOW_MIN'' then PARAMETER_1 end) FLOW_MIN,
        max(case when CODE= ''COS_MIN'' then PARAMETER_1 end) COS_MIN,
        max(case when CODE= ''COS_MAX'' then PARAMETER_1 end) COS_MAX,
        max(case when CODE= ''ANALYZE_MONTH_RANGE'' then PARAMETER_1 end) ANALYZE_MONTH_RANGE,
        max(case when CODE= ''STEP_CLOCK'' then PARAMETER_1 end)-1 STEP_CLOCK,
        max(case when CODE= ''SCORE'' then PARAMETER_1 end) SCORE INTO V_SABONE_TYPE,V_SFLOW_MIN,V_SCOS_MIN,V_SCOS_MAX,V_SANALYZE_MONTH_RANGE,V_SSTEP_CLOCK,V_SSCORE
      FROM   '||gv_edw_owner||'.'||v_src_table_02||' D 
      JOIN   '||gv_edw_owner||'.'||v_src_table_03||' G ON (D.RID = G.MASTER_ID) 
      WHERE
        D.D_CODE =''subscribe_Analyze''
        AND D.D_DESCRIPTION =''COS_DEGERI_DUSUK_OLAN_Sekonder''
    ';

    execute immediate gv_dyn_task;

    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS   
    SELECT 
      WIRINGNO,
      PROFILEDATE,
      V1,
      V2,
      V3,
      I1,
      I2,
      I3,
      COS1,
      COS2,
      COS3,
      FLOWMULTIPLIER,
      VOLTAGEMULTIPLIER,
      ABONETYPE,
      MBS_MULTIPLIER,
      SAYAC_NO ,
      SUBSTR(PROFILEDATE,1,8) ROW_DATE
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' L  
    WHERE 
      NVL(L.SECTORINFO,'''') NOT LIKE ''%TAR%''
      AND L.ABONETYPE =''S''
      AND TO_NUMBER(SUBSTR(L.PROFILEDATE,1,8)) >=TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE,-3),''YYYYMMDD''))
      AND 
      (
        (  
          (L.COS1 BETWEEN '||V_SCOS_MIN||' AND '||V_SCOS_MAX||' AND L.C1NEXT BETWEEN '||V_SCOS_MIN||' AND '||V_SCOS_MAX||' AND L.C1NEXTNEXT BETWEEN '||V_SCOS_MIN||' AND '||V_SCOS_MAX||' )  
          OR 
          (L.COS2 BETWEEN '||V_SCOS_MIN||' AND '||V_SCOS_MAX||' AND L.C2NEXT BETWEEN '||V_SCOS_MIN||' AND '||V_SCOS_MAX||' AND L.C2NEXTNEXT BETWEEN '||V_SCOS_MIN||' AND '||V_SCOS_MAX||') 
          OR 
          (L.COS3 BETWEEN '||V_SCOS_MIN||' AND '||V_SCOS_MAX||' AND L.C3NEXT BETWEEN '||V_SCOS_MIN||' AND '||V_SCOS_MAX||' AND L.C3NEXTNEXT BETWEEN '||V_SCOS_MIN||' AND '||V_SCOS_MAX||')
         )
          AND (I1> '||V_SFLOW_MIN||'  AND I2> '||V_SFLOW_MIN||' AND I3> '||V_SFLOW_MIN||')
      )            
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  


PROCEDURE PRC_TMP_COS_TR0030
  IS
    ----------------------------------------------------------------------------
    v_table_name            varchar2(35) := 'COS_TR_TAG003';
    ----------------------------------------------------------------------------
    v_src_table_01          varchar2(60) := 'TMP_RAWDATA0010_C';
    v_src_table_02          varchar2(60) := 'D_DEFINITIONS';
    v_src_table_03          varchar2(60) := 'D_GROUPS';
    V_TABONE_TYPE           VARCHAR2(20) :=      NULL;
    V_TFLOW_MIN             VARCHAR2(10) :=      NULL;
    V_TCOS_MIN              VARCHAR2(10) :=      NULL;
    V_TCOS_MAX              VARCHAR2(10) :=      NULL;
    V_TANALYZE_MONTH_RANGE  VARCHAR2(10) :=      NULL;
    V_TSTEP_CLOCK           VARCHAR2(10) :=      NULL;
    V_TSCORE                VARCHAR2(10) :=      NULL;

    ----------------------------------------------------------------------------
BEGIN
    gv_proc := 'PRC_TMP_COS_TR0030';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
       

    plib.drop_table(gv_stg_owner, v_table_name);

    gv_dyn_task := '            
    SELECT 
      max(case when CODE= ''ABONE_TYPE'' then PARAMETER_1 end) ABONE_TYPE,
      max(case when CODE= ''FLOW_MIN'' then PARAMETER_1 end) FLOW_MIN,
      max(case when CODE= ''COS_MIN'' then PARAMETER_1 end) COS_MIN,
      max(case when CODE= ''COS_MAX'' then PARAMETER_1 end) COS_MAX,
      max(case when CODE= ''ANALYZE_MONTH_RANGE'' then PARAMETER_1 end) ANALYZE_MONTH_RANGE,
      max(case when CODE= ''STEP_CLOCK'' then PARAMETER_1 end)-1 STEP_CLOCK,
      max(case when CODE= ''SCORE'' then PARAMETER_1 end) SCORE  
    INTO  
      V_TABONE_TYPE,
      V_TFLOW_MIN,
      V_TCOS_MIN,
      V_TCOS_MAX,
      V_TANALYZE_MONTH_RANGE,
      V_TSTEP_CLOCK,
      V_TSCORE
      FROM   '||gv_edw_owner||'.'||v_src_table_02||' D 
      JOIN   '||gv_edw_owner||'.'||v_src_table_03||' G ON (D.RID = G.MASTER_ID) 
    WHERE 
      D.D_CODE =''subscribe_Analyze''
      AND D.D_DESCRIPTION =''COS_DEGERI_DUSUK_OLAN_Tarımsal_Sulama''
    ';

    execute immediate gv_dyn_task;
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS   
    SELECT /*+ PARALLEL(L,16) */
      WIRINGNO,
      PROFILEDATE,
      V1,
      V2,
      V3,
      I1,
      I2,
      I3,
      COS1,
      COS2,
      COS3,
      FLOWMULTIPLIER,
      VOLTAGEMULTIPLIER,
      ABONETYPE,
      MBS_MULTIPLIER,
      SAYAC_NO,
      SUBSTR(PROFILEDATE,1,8) ROW_DATE
     FROM
       '||gv_stg_owner||'.'||v_src_table_01||' L  
     WHERE 
       NVL(L.SECTORINFO,'''')  LIKE ''%TAR%''
       AND 
       (
           (L.COS1 BETWEEN '||V_TCOS_MIN||' AND '||V_TCOS_MAX||' AND L.C1NEXT BETWEEN '||V_TCOS_MIN||' AND '||V_TCOS_MAX||' AND L.C1NEXTNEXT BETWEEN '||V_TCOS_MIN||' AND '||V_TCOS_MAX||')  
        OR (L.COS2 BETWEEN '||V_TCOS_MIN||' AND '||V_TCOS_MAX||' AND L.C2NEXT BETWEEN '||V_TCOS_MIN||' AND '||V_TCOS_MAX||' AND L.C2NEXTNEXT BETWEEN '||V_TCOS_MIN||' AND '||V_TCOS_MAX||') 
        OR (L.COS3 BETWEEN '||V_TCOS_MIN||' AND '||V_TCOS_MAX||' AND L.C3NEXT BETWEEN '||V_TCOS_MIN||' AND '||V_TCOS_MAX||' AND L.C3NEXTNEXT BETWEEN '||V_TCOS_MIN||' AND '||V_TCOS_MAX||')
       )
        AND 
       (
            ((L.FLOWMULTIPLIER)*L.I1>'||V_TFLOW_MIN||') 
        AND ((L.FLOWMULTIPLIER)*L.I2>'||V_TFLOW_MIN||') 
        AND ((L.FLOWMULTIPLIER)*L.I3>'||V_TFLOW_MIN||')
       )
          
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
    
  
  
   PROCEDURE PRC_TMP_COS_0030
  IS
    ----------------------------------------------------------------------------
    v_table_name            varchar2(35) := 'COS_TAG003';
    v_src_table_01          varchar2(60) := 'COS_P_TAG003';
    v_src_table_02          varchar2(60) := 'COS_S_TAG003';
    v_src_table_03          varchar2(60) := 'COS_TR_TAG003';
    v_src_table_04          varchar2(60) := 'D_DEFINITIONS';
    v_src_table_05          varchar2(60) := 'D_GROUPS';
    V_PSCORE                VARCHAR2(10) :=      NULL;
    V_SSCORE                VARCHAR2(10) :=      NULL;
    V_TSCORE                VARCHAR2(10) :=      NULL;
    V_PSTEP_CLOCK           VARCHAR2(10) :=      NULL;
    V_SSTEP_CLOCK           VARCHAR2(10) :=      NULL;
    V_TSTEP_CLOCK           VARCHAR2(10) :=      NULL;
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_COS_0030';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
       

    plib.drop_table(gv_stg_owner, v_table_name);
     
  
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS   
    SELECT 
      WIRINGNO,
      PROFILEDATE,
      V1,
      V2,
      V3,
      I1,
      I2,
      I3,
      COS1,
      COS2,
      COS3,
      FLOWMULTIPLIER,
      VOLTAGEMULTIPLIER,
      ABONETYPE,
      SCORE,
      TAGTYPE,
      ROW_DATE
      FROM
      (
      SELECT 
        WIRINGNO,
        PROFILEDATE,
        V1,
        V2,
        V3,
        I1,
        I2,
        I3,
        COS1,
        COS2,
        COS3,
        FLOWMULTIPLIER,
        VOLTAGEMULTIPLIER,
        ABONETYPE,
        5 AS SCORE,
        3 AS TAGTYPE,
        ROW_DATE,
        ROW_NUMBER() OVER ( PARTITION BY WIRINGNO,ROW_DATE ORDER BY PROFILEDATE DESC ) ROW_NUM
        
      FROM 
        '||gv_stg_owner||'.'||v_src_table_01||' 
      )    
      WHERE 
        ROW_NUM =1
    
    UNION ALL
    
    SELECT 
      WIRINGNO,
      PROFILEDATE,
      V1,
      V2,
      V3,
      I1,
      I2,
      I3,
      COS1,
      COS2,
      COS3,
      FLOWMULTIPLIER,
      VOLTAGEMULTIPLIER,
      ABONETYPE,
      SCORE,
      TAGTYPE,
      ROW_DATE
      FROM
      (
      SELECT 
        WIRINGNO,
        PROFILEDATE,
        V1,
        V2,
        V3,
        I1,
        I2,
        I3,
        COS1,
        COS2,
        COS3,
        FLOWMULTIPLIER,
        VOLTAGEMULTIPLIER,
        ABONETYPE,
        5 AS SCORE,
        3 AS TAGTYPE,
        ROW_DATE,
        ROW_NUMBER() OVER ( PARTITION BY WIRINGNO,ROW_DATE ORDER BY PROFILEDATE DESC ) ROW_NUM
        
      FROM 
        '||gv_stg_owner||'.'||v_src_table_02||' 
      )    
      WHERE 
        ROW_NUM =1
    UNION ALL
    
    SELECT 
      WIRINGNO,
      PROFILEDATE,
      V1,
      V2,
      V3,
      I1,
      I2,
      I3,
      COS1,
      COS2,
      COS3,
      FLOWMULTIPLIER,
      VOLTAGEMULTIPLIER,
      ABONETYPE,
      SCORE,
      TAGTYPE,
      ROW_DATE
      FROM
      (
      SELECT 
        WIRINGNO,
        PROFILEDATE,
        V1,
        V2,
        V3,
        I1,
        I2,
        I3,
        COS1,
        COS2,
        COS3,
        FLOWMULTIPLIER,
        VOLTAGEMULTIPLIER,
        ABONETYPE,
        5 AS SCORE,
        3 AS TAGTYPE,
        ROW_DATE,
        ROW_NUMBER() OVER ( PARTITION BY WIRINGNO,ROW_DATE ORDER BY PROFILEDATE DESC ) ROW_NUM
        
      FROM 
        '||gv_stg_owner||'.'||v_src_table_03||' 
      )    
      WHERE 
        ROW_NUM =1
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  PROCEDURE PRC_TMP_ENDEX_FALL0040
  IS
    ----------------------------------------------------------------------------
    v_table_name       varchar2(35) := 'ENDEX_TAG004';
    ----------------------------------------------------------------------------
    v_src_table_01     varchar2(60) := 'TMP_RAWDATA0010_C';
    V_ABONE_TYPE       CHAR(1)      := NULL;
    V_ENDEX_DIFFERENCE VARCHAR2(10) := NULL;
    V_SCORE            VARCHAR2(10) := NULL;  
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_ENDEX_FALL0040';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
      
    SELECT 
      max(case when CODE= 'ABONE_TYPE' then PARAMETER_1 end) ABONE_TYPE,
      max(case when CODE= 'ENDEX_DIFFERENCE' then PARAMETER_1 end) ENDEX_DIFFERENCE,
      max(case when CODE= 'SCORE' then PARAMETER_1 end) SCORE INTO  V_ABONE_TYPE,V_ENDEX_DIFFERENCE,V_SCORE
    FROM 
      DWH_EDW.D_DEFINITIONS D 
      JOIN DWH_EDW.D_GROUPS G ON (D.RID = G.MASTER_ID) 
    WHERE
       D.D_CODE ='subscribe_Analyze'
      AND D.D_DESCRIPTION ='ENDEKS_DUSMESI_Primer';
    

    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT 
      T.WIRINGNO,
      T.PROFILEDATE,
      T.V1,
      T.V2,
      T.V3,
      T.I1,
      T.I2,
      T.I3,
      T.COS1,
      T.COS2,
      T.COS3,
      T.ENDEX,
      T.NEXTENDEX,
      T.FLOWMULTIPLIER,
      T.VOLTAGEMULTIPLIER,
      T.ABONETYPE,
      (T.NEXTENDEX-T.ENDEX) ENDEXFARK,
      '||V_SCORE||' AS SCORE ,
      4 AS TAGTYPE,
      SUBSTR(PROFILEDATE,1,8) ROW_DATE
    FROM
      '||gv_stg_owner||'.'||v_src_table_01||' T
    WHERE
      (NEXTENDEX-ENDEX)<- '||V_ENDEX_DIFFERENCE||' 
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  PROCEDURE PRC_TMP_COMMAND_SYSTEM0050
  IS
    ----------------------------------------------------------------------------
    v_table_name       varchar2(35) := 'COMMAND_TAG005';
    ----------------------------------------------------------------------------
    v_src_table_01     varchar2(60) := 'TMP_RAWDATA0010_C';
    V_TABONE_TYPE      VARCHAR2(20):=  NULL;
    V_TSTEP_CLOCK      VARCHAR2(10):=  NULL;
    V_TSTART_DATE      VARCHAR2(10):=  NULL;
    V_TSCORE           VARCHAR2(10):=  NULL;
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_COMMAND_SYSTEM0050';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT /*+PARALLEL (16)*/ 
      WIRINGNO,
      PROFILEDATE,
      V1,
      V2,
      V3,
      I1,
      I2,
      I3,
      COS1,
      COS2,
      COS3,
      FLOWMULTIPLIER,
      VOLTAGEMULTIPLIER,
      ABONETYPE,
      MBS_MULTIPLIER,
      SAYAC_NO,
      ROW_DATE,
      5 AS TAGTYPE,
      5 AS SCORE
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' 
    WHERE 
      NEXTENDEX = ENDEX AND NEXTENDEX=NEXTNEXTENDEX AND   NVL(SECTORINFO,'''')  LIKE ''%TAR%''
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  
   PROCEDURE PRC_TMPKCK_0060
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMPKCK_0060';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'KACAK';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMPKCK_0060';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS      
    SELECT   /*+ PARALLEL(K 16)  */
      K.TESISAT_NO,
      MAX(K.ZABIT_TARIHI) KACAKTARIHI
    FROM 
      '||gv_ods_mbs_owner||'.'||v_src_table_01||' K 
    WHERE 
      K.ZABIT_SERI != ''Y'' AND
      K.IPTAL_TARIHI = 0 AND 
      ZABIT_TARIHI>20130100
    GROUP BY 
      K.TESISAT_NO
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  --------------------------------------------------------------------------------
  ------------------------------------------------------------------------------
  --  Created By     : �akir �ilo�lu
  --  Creation Date  : 2017.06.28
  --  Procedure Name : PRC_TMPKCK0020
  --  Description    : KACAK �NCES� SON FATURA ORTALAMA G�NL�K T�KET�M�
  -----------------------------------------------------------------------------
  
  PROCEDURE PRC_TMPKCK_0061
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMPKCK_0061';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'MBS_FATURA';
    v_src_table_02  varchar2(30) := 'TMPKCK_0060';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMPKCK_0061';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS      
    SELECT /*+ PARALLEL(16)  */
      TESISAT_NO,
      OKUMA_TARIHI KACAKONCESISONOKUMA,
      KACAKTARIHI,
      (CASE WHEN  (TO_DATE(OKUMA_TARIHI,''YYYYMMDD'')- TO_DATE(ILK_OKUMA_TARIHI,''YYYYMMDD''))>0
            THEN ROUND(((NVL(AKTIF_KWH,0)+NVL(SATICI_KWH,0))/(TO_DATE(OKUMA_TARIHI,''YYYYMMDD'')- TO_DATE(ILK_OKUMA_TARIHI,''YYYYMMDD''))),2)
                ELSE ROUND((NVL(AKTIF_KWH,0)+NVL(SATICI_KWH,0)),2) END) AS KACAKONCESIGUNLUKKWH
    FROM
      (
      SELECT  /*+ PARALLEL(F 16) PARALLEL(A 16)  */
        A.TESISAT_NO,
        F.OKUMA_TARIHI,
        F.ILK_OKUMA_TARIHI,
        AKTIF_KWH,
        SATICI_KWH,
        KACAKTARIHI,
        ROW_NUMBER() OVER(PARTITION BY F.TESISAT_NO ORDER BY F.OKUMA_TARIHI DESC) RN 
      FROM 
        '||gv_ods_owner||'.'||v_src_table_01||'  F,
        '||gv_stg_owner||'.'||v_src_table_02||'  A
      WHERE 
        A.TESISAT_NO=F.TESISAT_NO(+) AND 
        A.KACAKTARIHI>F.OKUMA_TARIHI AND 
        F.FATURA_KODU NOT IN (2,9) AND 
        F.FATURA_TIPI!=1 AND  
        EXISTS (
               SELECT  /*+ PARALLEL(A 16)  */
                 TESISAT_NO 
               FROM 
                 '||gv_stg_owner||'.'||v_src_table_02||' A 
               WHERE 
                 A.TESISAT_NO=F.TESISAT_NO
               )
      ) 
    WHERE RN=1
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  
  --------------------------------------------------------------------------------
  ------------------------------------------------------------------------------
  --  Created By     : �akir �ilo�lu
  --  Creation Date  : 2017.06.28
  --  Procedure Name : PRC_TMPKCK0030
  --  Description    : KACAK SONRASI ILK FATURA ORTALAMA G�NL�K DE�ER�
  -----------------------------------------------------------------------------
  
  PROCEDURE PRC_TMPKCK_0062
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMPKCK_0062';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'MBS_FATURA';
    v_src_table_02  varchar2(30) := 'TMPKCK_0060';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMPKCK_0062';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
      CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
      PARALLEL NOLOGGING COMPRESS
      AS      
      SELECT /*+ PARALLEL(16)  */
        TESISAT_NO,
        OKUMA_TARIHI KACAKSONRASIILKOKUMA,
        KACAKTARIHI,
        (CASE WHEN  (TO_DATE(OKUMA_TARIHI,''YYYYMMDD'')- TO_DATE(ILK_OKUMA_TARIHI,''YYYYMMDD''))>0
                THEN ROUND(((NVL(AKTIF_KWH,0)+NVL(SATICI_KWH,0))/(TO_DATE(OKUMA_TARIHI,''YYYYMMDD'')- TO_DATE(ILK_OKUMA_TARIHI,''YYYYMMDD''))),2)
                    ELSE ROUND((NVL(AKTIF_KWH,0)+NVL(SATICI_KWH,0)),2) END) AS KACAKSONRASIILKGUNLUKKWH
      FROM
        (
        SELECT  /*+ PARALLEL(F 16) PARALLEL(A 16)  */
          A.TESISAT_NO,
          F.OKUMA_TARIHI,
          F.ILK_OKUMA_TARIHI,
          AKTIF_KWH,
          SATICI_KWH,
          KACAKTARIHI,
          ROW_NUMBER() OVER(PARTITION BY F.TESISAT_NO ORDER BY F.OKUMA_TARIHI ASC) RN 
        FROM 
          '||gv_ods_owner||'.'||v_src_table_01||'  F,
          '||gv_stg_owner||'.'||v_src_table_02||'  A
        WHERE 
          A.TESISAT_NO=F.TESISAT_NO(+) AND 
          A.KACAKTARIHI<F.OKUMA_TARIHI AND 
          F.FATURA_KODU NOT IN (2,9) AND 
          F.FATURA_TIPI!=1 AND  
          EXISTS ( 
                 SELECT  /*+ PARALLEL(A 16)  */
                   TESISAT_NO
                 FROM 
                   '||gv_stg_owner||'.'||v_src_table_02||' A 
                 WHERE
                   A.TESISAT_NO=F.TESISAT_NO
                 )
        ) 
      WHERE RN=1
      ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;


 --------------------------------------------------------------------------------
  ------------------------------------------------------------------------------
  --  Created By     : �akir �ilo�lu
  --  Creation Date  : 2017.06.28
  --  Procedure Name : PRC_TMPKCK0040
  --  Description    : KACAK SONRASI SON FATURA ORTALAMA G�NL�K T�KET�M�
  -----------------------------------------------------------------------------
  
  PROCEDURE PRC_TMPKCK_0063
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMPKCK_0063';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'MBS_FATURA';
    v_src_table_02  varchar2(30) := 'TMPKCK_0060';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMPKCK_0063';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
      CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
      PARALLEL NOLOGGING COMPRESS
      AS      
      SELECT /*+ PARALLEL(16) */
        TESISAT_NO,
        OKUMA_TARIHI KACAKSONRASISONOKUMA,
        KACAKTARIHI,
        (CASE WHEN  (TO_DATE(OKUMA_TARIHI,''YYYYMMDD'')- TO_DATE(ILK_OKUMA_TARIHI,''YYYYMMDD''))>0
                THEN ROUND(((NVL(AKTIF_KWH,0)+NVL(SATICI_KWH,0))/(TO_DATE(OKUMA_TARIHI,''YYYYMMDD'')- TO_DATE(ILK_OKUMA_TARIHI,''YYYYMMDD''))),2)
                    ELSE ROUND((NVL(AKTIF_KWH,0)+NVL(SATICI_KWH,0)),2) END) AS KACAKSONRASISONGUNLUKKWH
      FROM
        (
        SELECT  /*+ PARALLEL(F 16) PARALLEL(A 16)  */
          A.TESISAT_NO,
          F.OKUMA_TARIHI,
          F.ILK_OKUMA_TARIHI,
          AKTIF_KWH,
          SATICI_KWH,
          KACAKTARIHI,
          ROW_NUMBER() OVER(PARTITION BY F.TESISAT_NO ORDER BY F.OKUMA_TARIHI DESC) RN 
        FROM 
          '||gv_ods_owner||'.'||v_src_table_01||'  F,
          '||gv_stg_owner||'.'||v_src_table_02||'  A
        WHERE 
          A.TESISAT_NO=F.TESISAT_NO(+) AND 
          A.KACAKTARIHI<F.OKUMA_TARIHI AND 
          F.FATURA_KODU NOT IN (2,9) AND 
          F.FATURA_TIPI!=1 AND  
          EXISTS (
                 SELECT  /*+ PARALLEL(A 16)*/
                   TESISAT_NO 
                 FROM '||gv_stg_owner||'.'||v_src_table_02||' A 
                 WHERE
                   A.TESISAT_NO=F.TESISAT_NO
                 )
        ) 
      WHERE RN=1
          ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  
  ------------------------------------------------------------------------------

  ------------------------------------------------------------------------------
  --  Created By     : �akir �ilo�lu
  --  Creation Date  : 2017.06.28
  --  Procedure Name : PRC_KACAK_ANALIZ_SUMMARY
  --  Description    : KACAK ONCEI SONRASI VE EN SON T�KET�MLERE �L��K�N T�KET�M
  -- KIYASININ YAPILDI�I �ZET TABLO OLU�TURULUR.
  --  [Modification History]
  --  -------------------------------------------------------------------------
  
  PROCEDURE PRC_LEAKAGEINVOICE_0060
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'LEAKAGEINVOICE_TAG006';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'D_SUBSCRIBERS';
    v_src_table_02  varchar2(30) := 'TMPKCK_0061';
    v_src_table_03  varchar2(30) := 'TMPKCK_0062';
    v_src_table_04  varchar2(30) := 'TMPKCK_0063';
    
    
    ----------------------------------------------------------------------------
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_LEAKAGEINVOICE_0060';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT /*+ PARALLEL(E 16) PARALLEL(B 16) PARALLEL(C 16) PARALLEL(D 16) */
      B.TESISAT_NO,
      B.KACAKONCESISONOKUMA,
      B.KACAKTARIHI,
      B.KACAKONCESIGUNLUKKWH KO_KWH,
      C.KACAKSONRASIILKOKUMA,
      C.KACAKSONRASIILKGUNLUKKWH KSI_KWH,
      D.KACAKSONRASISONOKUMA,
      D.KACAKSONRASISONGUNLUKKWH KSS_KWH,
      E.NAME as UNVAN,
      E.OSOS_STATUS as OSOSDUR,
      E.OG_STATUS as OGDUR,
      E.INSTALLED_POWER as KURULUGUC,
      E.RATION_CARD_ID as KARNENO,
      E.PROVINCE as ILADI,
      E.AREA_NAME as BOLGEADI,
      E.SUBSCRIBER_GROUP_NAME as ABONEGRUPADI,
      E.SUBSCRIBER_STATUS_NAME as ABONEDURUMADI,
      CASE WHEN KACAKSONRASIILKGUNLUKKWH>KACAKSONRASISONGUNLUKKWH*(1.5) THEN 0 ELSE 1 END KSONRAILK_ENSON, -- 0 KACAK SONRASI ILK FATURA EN SON FATURADAN KUCUK DEMEK
      CASE WHEN KACAKSONRASISONGUNLUKKWH>KACAKONCESIGUNLUKKWH*(1.5) THEN 1 ELSE 0     END SONFATURA_KO_BUYUKMU, -- 0 SON FATURA KACAK ONCESINDEN KUCUK DEMEK 
      CASE WHEN (1.5)*KACAKONCESIGUNLUKKWH<KACAKSONRASIILKGUNLUKKWH   THEN 1 ELSE 0   END KACAKSONRASI_ARTIS, -- 0 KACAK SONRASI ARTIS YETERLI DEGIL DEMEK
      6 AS TAGTYPE
    FROM 
      '||gv_edw_owner||'.'||v_src_table_01||' E,
      '||gv_stg_owner||'.'||v_src_table_02||' B,
      '||gv_stg_owner||'.'||v_src_table_03||' C,
      '||gv_stg_owner||'.'||v_src_table_04||' D
    WHERE 
      E.SUBSCRIBER_ID=B.TESISAT_NO AND 
      E.SUBSCRIBER_ID=C.TESISAT_NO AND 
      E.SUBSCRIBER_ID=D.TESISAT_NO     
    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  ------------------------------------------------------------------------------

  ------------------------------------------------------------------------------
  --  Created By     : �akir �ilo�lu
  --  Creation Date  : 2017.06.28
  --  Procedure Name : PRC_TMPSYC0010
  --  Description    : ABONELER�N MEVCUT SAYA�LARININ DE���ME TAR�HLER�N� ��EREN 
  --  TEMP TABLOYU OLU�TURUR.
  --  [Modification History]
  --  -------------------------------------------------------------------------

  
  PROCEDURE PRC_TMPSYC0070
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMPSYC0070';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'SAYAC';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMPSYC0070';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT  /*+ PARALLEL(S 16)*/
      S.TESISAT_NO,
      S.TAKILMA_TARIHI
    FROM 
      '||gv_ods_mbs_owner||'.'||v_src_table_01||'  S 
    WHERE 
      S.SAYAC_KODU=1       
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
      ------------------------------------------------------------------------------

  ------------------------------------------------------------------------------
  --  Created By     : �akir �ilo�lu
  --  Creation Date  : 2017.06.28
  --  Procedure Name : PRC_TMPSYC0020
  --  Description    : SAYA� DE����M� �NCES� �IKAN SON FATURANIN ORTALAMA G�NL�K
  --  DE�ER�N� HESAPLAYAN TEMP TABLOYU OLU�TURUR.
  --  [Modification History]
  --  -------------------------------------------------------------------------

  
  PROCEDURE PRC_TMPSYC0071
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMPSYC0071';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'MBS_FATURA';
    v_src_table_02  varchar2(30) := 'TMPSYC0070';
    
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMPSYC0071';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT /*+ PARALLEL(16)*/
      TESISAT_NO,
      TAKILMA_TARIHI,
      OKUMA_TARIHI SAYACONCESISONOKUMA,
      (CASE WHEN  (TO_DATE(OKUMA_TARIHI,''YYYYMMDD'')- TO_DATE(ILK_OKUMA_TARIHI,''YYYYMMDD''))>0
              THEN ROUND(((NVL(AKTIF_KWH,0)+NVL(SATICI_KWH,0))/(TO_DATE(OKUMA_TARIHI,''YYYYMMDD'')- TO_DATE(ILK_OKUMA_TARIHI,''YYYYMMDD''))),2)
                  ELSE ROUND((NVL(AKTIF_KWH,0)+NVL(SATICI_KWH,0)),2) END) AS SAYACONCESISONGUNLUK
    FROM
      (
      SELECT  /*+ PARALLEL(F 16) PARALLEL(A 16)  */
        A.TESISAT_NO,
        F.OKUMA_TARIHI,
        F.ILK_OKUMA_TARIHI,
        AKTIF_KWH,
        SATICI_KWH,
        TAKILMA_TARIHI,
        ROW_NUMBER() OVER(PARTITION BY F.TESISAT_NO ORDER BY F.OKUMA_TARIHI DESC) RN 
      FROM 
      '||gv_ods_owner||'.'||v_src_table_01||'  F,
      '||gv_stg_owner||'.'||v_src_table_02||'  A
      WHERE 
        A.TESISAT_NO=F.TESISAT_NO(+) AND A.TAKILMA_TARIHI>F.OKUMA_TARIHI AND F.FATURA_KODU NOT IN (2,9) AND F.FATURA_TIPI!=1
        AND  
        EXISTS (
               SELECT 
                 TESISAT_NO
               FROM 
                 '||gv_stg_owner||'.'||v_src_table_02||' A 
               WHERE 
                 A.TESISAT_NO=F.TESISAT_NO
               )
      ) 
    WHERE RN=1     
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
   ------------------------------------------------------------------------------

  ------------------------------------------------------------------------------
  --  Created By     : �akir �ilo�lu
  --  Creation Date  : 2017.06.28
  --  Procedure Name : PRC_TMPSYC0030
  --  Description    : SAYA� DE����M� SONRASI �IKAN �LK FATURANIN ORTALAMA G�NL�K
  --  DE�ER�N� HESAPLAYAN TEMP TABLOYU OLU�TURUR.
  --  [Modification History]
  --  -------------------------------------------------------------------------

  
  PROCEDURE PRC_TMPSYC0072
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMPSYC0072';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'MBS_FATURA';
    v_src_table_02  varchar2(30) := 'TMPSYC0070';
    
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMPSYC0072';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT /*+ PARALLEL(16)*/
      TESISAT_NO,
      TAKILMA_TARIHI,
      OKUMA_TARIHI SAYACSONRASIILKOKUMA ,
      (CASE WHEN  (TO_DATE(OKUMA_TARIHI,''YYYYMMDD'')- TO_DATE(ILK_OKUMA_TARIHI,''YYYYMMDD''))>0
            THEN ROUND(((NVL(AKTIF_KWH,0)+NVL(SATICI_KWH,0))/(TO_DATE(OKUMA_TARIHI,''YYYYMMDD'')- TO_DATE(ILK_OKUMA_TARIHI,''YYYYMMDD''))),2)
            ELSE ROUND((NVL(AKTIF_KWH,0)+NVL(SATICI_KWH,0)),2) END) AS SAYACSONRASIILKGUNLUK
    FROM
      (
      SELECT /*+ PARALLEL(F 16) PARALLEL(A 16)  */
        A.TESISAT_NO,
        F.OKUMA_TARIHI,
        F.ILK_OKUMA_TARIHI,
        AKTIF_KWH,
        SATICI_KWH,
        TAKILMA_TARIHI,
        ROW_NUMBER() OVER(PARTITION BY F.TESISAT_NO ORDER BY F.OKUMA_TARIHI ASC) RN 
      FROM 
       '||gv_ods_owner||'.'||v_src_table_01||'  F,
       '||gv_stg_owner||'.'||v_src_table_02||'  A
      WHERE
        A.TESISAT_NO=F.TESISAT_NO(+) AND
        A.TAKILMA_TARIHI<F.OKUMA_TARIHI AND
        F.FATURA_KODU NOT IN (2,9) AND 
        F.FATURA_TIPI!=1 AND  
        EXISTS (
               SELECT
                 TESISAT_NO 
               FROM 
                 '||gv_stg_owner||'.'||v_src_table_02||' A 
               WHERE 
                 A.TESISAT_NO=F.TESISAT_NO
               )
      ) 
    WHERE RN=1
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  
  PROCEDURE PRC_TMPSYC0073
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMPSYC0073';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'MBS_FATURA';
    v_src_table_02  varchar2(30) := 'TMPSYC0070';
    
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMPSYC0073';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT /*+ PARALLEL(16)*/
      TESISAT_NO,
      TAKILMA_TARIHI,
      OKUMA_TARIHI SAYACSONRASISONOKUMA,
      (CASE WHEN  (TO_DATE(OKUMA_TARIHI,''YYYYMMDD'')- TO_DATE(ILK_OKUMA_TARIHI,''YYYYMMDD''))>0
              THEN ROUND(((NVL(AKTIF_KWH,0)+NVL(SATICI_KWH,0))/(TO_DATE(OKUMA_TARIHI,''YYYYMMDD'')- TO_DATE(ILK_OKUMA_TARIHI,''YYYYMMDD''))),2)
                  ELSE ROUND((NVL(AKTIF_KWH,0)+NVL(SATICI_KWH,0)),2) END) AS SAYACSONRASISONGUNLUK
    FROM
      (
      SELECT   /*+ PARALLEL(F 16) PARALLEL(A 16)*/
        A.TESISAT_NO,
        F.OKUMA_TARIHI,
        F.ILK_OKUMA_TARIHI,
        AKTIF_KWH,
        SATICI_KWH,
        TAKILMA_TARIHI,
        ROW_NUMBER() OVER(PARTITION BY F.TESISAT_NO ORDER BY F.OKUMA_TARIHI DESC) RN 
      FROM 
        '||gv_ods_owner||'.'||v_src_table_01||'  F,
        '||gv_stg_owner||'.'||v_src_table_02||'  A 
      WHERE 
        A.TESISAT_NO=F.TESISAT_NO(+) AND 
        A.TAKILMA_TARIHI<F.OKUMA_TARIHI AND 
        F.FATURA_KODU NOT IN (2,9) AND 
        F.FATURA_TIPI!=1 AND  
        EXISTS (
               SELECT 
                 TESISAT_NO 
               FROM 
                 '||gv_stg_owner||'.'||v_src_table_02||' A 
               WHERE 
                 A.TESISAT_NO=F.TESISAT_NO
               )
      ) 
    WHERE RN=1    
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
 
 
 
 
  PROCEDURE PRC_METERINVOICE_0070
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'METERINVOICE_TAG007';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'D_SUBSCRIBERS';
    v_src_table_02  varchar2(30) := 'TMPSYC0071';
    v_src_table_03  varchar2(30) := 'TMPSYC0072';
    v_src_table_04  varchar2(30) := 'TMPSYC0073';
    
    
    ----------------------------------------------------------------------------
     BEGIN
    gv_proc := 'PRC_METERINVOICE_0070';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT /*+ PARALLEL(E 16) PARALLEL(B 16) PARALLEL(S 16) PARALLEL(D 16) */
      E.NAME as UNVAN,
      E.OSOS_STATUS as OSOSDUR,
      E.OG_STATUS as OGDUR,
      E.INSTALLED_POWER as KURULUGUC,
      E.RATION_CARD_ID as KARNENO,
      E.PROVINCE as ILADI,
      E.AREA_NAME as BOLGEADI,
      E.SUBSCRIBER_GROUP_NAME as ABONEGRUPADI,
      E.SUBSCRIBER_STATUS_NAME as ABONEDURUMADI,
      B.TESISAT_NO,
      B.TAKILMA_TARIHI,
      B.SAYACONCESISONOKUMA,
      B.SAYACONCESISONGUNLUK SO_KWH,
      C.SAYACSONRASIILKOKUMA,
      C.SAYACSONRASIILKGUNLUK SSI_KWH,
      D.SAYACSONRASISONOKUMA,
      D.SAYACSONRASISONGUNLUK SSS_KWH ,
      CASE WHEN SAYACSONRASIILKGUNLUK>SAYACONCESISONGUNLUK*1.5 AND SAYACSONRASIILKGUNLUK>SAYACSONRASISONGUNLUK*1.50 THEN 1 ELSE 0 END SAYACSONRASIARTISDUSUS,
      CASE WHEN SAYACSONRASIILKGUNLUK>SAYACONCESISONGUNLUK*1.5 THEN 1 ELSE 0 END SAYACSONRASIARTIS,
      7 AS TAGTYPE
    FROM 
      '||gv_edw_owner||'.'||v_src_table_01||' E,
      '||gv_stg_owner||'.'||v_src_table_02||' B,
      '||gv_stg_owner||'.'||v_src_table_03||' C,
      '||gv_stg_owner||'.'||v_src_table_04||' D 
    WHERE  
      E.SUBSCRIBER_ID=B.TESISAT_NO AND 
      E.SUBSCRIBER_ID=C.TESISAT_NO AND 
      E.SUBSCRIBER_ID=D.TESISAT_NO AND SAYACSONRASISONGUNLUK>0 AND SAYACONCESISONGUNLUK>0
     ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  
  -------------------------------------------------------------------------------
  --  Created By     : �akir �ilo�lu
  --  Creation Date  : 2017.06.28
  --  Procedure Name : PRC_TMPDEM0010
  --  Description    :ENDEKSOR OPTIK TOPLA VE ENDEKS B�LG� TABLOLARINDAK� B�R   
  --  TES�SATIN G�N ��ER�S�NDE ALDI�I EN B�Y�K DEMAND DE�ERLER� OKUMA TAR�H�NE  
  --  G�RE SIRALAR  EN B�Y�K TAR�HL� OLAN DEMAND DE�ERLER� ALIR. �K� TABLODAK� 
  --  DE�ERLER UNION �LE B�RLE�T�R�L�R. HANG� TABLODA DAHA YEN� KAYIT VAR �SE 
  --  O DEMAND DE�ER�N� TES�SATA SON DE�ER OLARAK ATAR. (SON DEMAND) 
  --  [Modification History]
  --  -------------------------------------------------------------------------

  
  PROCEDURE PRC_TMPDEM0080
  IS
    ---------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMPDEM0080';
    ---------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'ENDEKS_BILGI';
    v_src_table_02  varchar2(30) := 'ENDEKSOR_OPTIK_TOPLA';
   
  -----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMPDEM0080';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT 
      TESISAT_NO,
      OKUMA_TARIHI SONDEMANDTARIHI,
      DEMANT AS SONDEMAND
    FROM
      (
      SELECT 
        TESISAT_NO,
        OKUMA_TARIHI,
        DEMANT,
        ROW_NUMBER() OVER (PARTITION BY TESISAT_NO ORDER BY OKUMA_TARIHI DESC) SN
      FROM
        (
        SELECT  
          TESISAT_NO,
          OKUMA_TARIHI,
          CEKILEN_GUC AS DEMANT 
        FROM 
          (
          SELECT 
            E.TESISAT_NO,
            E.OKUMA_TARIHI, 
            E.CEKILEN_GUC,
            ROW_NUMBER() OVER (PARTITION BY E.TESISAT_NO ORDER BY E.OKUMA_TARIHI DESC) AS RN
          FROM 
            '||gv_ods_mbs_owner||'.'||v_src_table_01||' E 
          WHERE 
            E.CEKILEN_GUC IS NOT NULL  
          GROUP BY 
            E.TESISAT_NO,
            E.OKUMA_TARIHI ,
            E.CEKILEN_GUC
          ) K
        WHERE
          K.RN=1
        UNION ALL 
        SELECT 
          TESISAT_NO,
          OKUMA_TARIHI,
          DEMANT_GUCU*1000 AS DEMANT  
        FROM 
          (
          SELECT 
            E.TESISAT_NO,
            E.OKUMA_TARIHI, 
            E.DEMANT_GUCU,
            ROW_NUMBER() OVER (PARTITION BY E.TESISAT_NO ORDER BY E.OKUMA_TARIHI DESC) AS RN
          FROM 
            '||gv_ods_mbs_owner||'.'||v_src_table_02||'  E  
          WHERE
            E.DEMANT_GUCU>0 
          GROUP BY 
            E.TESISAT_NO,
            E.OKUMA_TARIHI , 
            E.DEMANT_GUCU
          ) K
        WHERE 
          K.RN=1
        )
      ) 
      WHERE SN=1
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  


 -------------------------------------------------------------------------------
  --  Created By     : �akir �ilo�lu
  --  Creation Date  : 2017.06.28
  --  Procedure Name : PRC_TMPDEM0020
  --  Description    :ENDEKSOR OPTIK TOPLA VE ENDEKS B�LG� TABLOLARINDAK� B�R   
  --  TES�SATIN G�N ��ER�S�NDE ALDI�I EN B�Y�K DEMAND DE�ERLER� OKUMA TAR�H�NE  
  --  G�RE SIRALAR  EN B�Y�K TAR�HL� OLAN DEMAND DE�ERLER� ALIR. �K� TABLODAK� 
  --  DE�ERLER UNION �LE B�RLE�T�R�L�R. HANG� TABLODA DAHA YEN� KAYIT VAR �SE 
  --  O DEMAND DE�ER�N� TES�SATA SON DE�ER OLARAK ATAR. (SON DEMAND) 
  --  [Modification History]
  --  -------------------------------------------------------------------------

  
  PROCEDURE PRC_TMPDEM0081
  IS
    ---------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMPDEM0081';
    ---------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'ENDEKS_BILGI';
    v_src_table_02  varchar2(30) := 'ENDEKSOR_OPTIK_TOPLA';
   
  -----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMPDEM0081';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT 
      TESISAT_NO,
      OKUMA_TARIHI MAXDEMANDTARIHI,
      DEMANT MAXDEMAND
    FROM
      (
      SELECT 
        TESISAT_NO,
        OKUMA_TARIHI, 
        DEMANT,
        ROW_NUMBER() OVER (PARTITION BY TESISAT_NO ORDER BY DEMANT DESC) SN 
      FROM
        ( 
        SELECT 
          TESISAT_NO,
          OKUMA_TARIHI,
          CEKILEN_GUC AS DEMANT 
        FROM 
          (
          SELECT 
            E.TESISAT_NO,
            E.OKUMA_TARIHI , 
            E.CEKILEN_GUC,
            ROW_NUMBER() OVER (PARTITION BY E.TESISAT_NO ORDER BY E.CEKILEN_GUC DESC) AS RN
          FROM 
            '||gv_ods_mbs_owner||'.'||v_src_table_01||' E  
          WHERE E.CEKILEN_GUC IS NOT NULL 
          GROUP BY
            E.TESISAT_NO,
            E.OKUMA_TARIHI , 
            E.CEKILEN_GUC
          )K
        WHERE K.RN=1
        UNION ALL
        SELECT 
          TESISAT_NO,
          OKUMA_TARIHI,
          DEMANT_GUCU*1000 AS DEMANT 
        FROM 
          (
          SELECT 
            E.TESISAT_NO,
            E.OKUMA_TARIHI , 
            E.DEMANT_GUCU,
            ROW_NUMBER() OVER (PARTITION BY E.TESISAT_NO ORDER BY E.DEMANT_GUCU DESC) AS RN
          FROM 
            '||gv_ods_mbs_owner||'.'||v_src_table_02||'  E 
          WHERE E.DEMANT_GUCU>0 
          GROUP BY 
            E.TESISAT_NO,
            E.OKUMA_TARIHI , 
            E.DEMANT_GUCU
          )K
        WHERE K.RN=1
        )
      ) 
    WHERE SN=1
      ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  
   -----------------------------------------------------------------------------
  --  Created By     : �akir �ilo�lu
  --  Creation Date  : 2017.06.28
  --  Procedure Name : PRC_TMPDEM0020
  --  Description    :ABONELER�N SON DEMAND VE MAXIMUM DEMANDLARI �ZER�NDEN 
  -- �E��TL� MATEMAT�KSEL ��LEMLERLE B�R �ZET TABLO OLU�TURULUR    
  -----------------------------------------------------------------------------
  
  
  
  PROCEDURE PRC_DEMAND_0080
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'DEMAND_TAG008';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'TMPDEM0080';
    v_src_table_02  varchar2(30) := 'TMPDEM0081';
    v_src_table_03  varchar2(30) := 'D_SUBSCRIBERS'; 
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_DEMAND_0080';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS 
    SELECT 
      S.TESISAT_NO,
      S.SONDEMANDTARIHI,
      S.SONDEMAND,
      M.MAXDEMANDTARIHI,
      M.MAXDEMAND,
      D.INSTALLED_POWER AS KURULUGUC,
      CASE WHEN SONDEMAND/MAXDEMAND>1    THEN  ''1_denBuyuk''
           WHEN SONDEMAND/MAXDEMAND=1    THEN  ''1''
           WHEN SONDEMAND/MAXDEMAND>0.50 THEN  ''1_0.50''
           WHEN SONDEMAND/MAXDEMAND>0.20 THEN  ''0.50_0.20''
           WHEN SONDEMAND/MAXDEMAND>0.10 THEN  ''0.20_0.10''
           WHEN SONDEMAND/MAXDEMAND>0    THEN  ''0.10_0.0'' END AS SONDEMAN_MAXDEMANDORAN,
      CASE WHEN MAXDEMAND/INSTALLED_POWER>2    THEN ''2 KatindanFazlaDemand'' 
           WHEN MAXDEMAND/INSTALLED_POWER>1    THEN ''MaxDemand>KG''
           WHEN MAXDEMAND/INSTALLED_POWER>0.75 THEN ''0.75_1''
           WHEN MAXDEMAND/INSTALLED_POWER>0.5  THEN ''0.5_0.75''
           WHEN MAXDEMAND/INSTALLED_POWER>0.3  THEN ''0.3_0.5''
           WHEN MAXDEMAND/INSTALLED_POWER>0.2  THEN ''0.2_0.3''
           WHEN MAXDEMAND/INSTALLED_POWER>0.1  THEN ''0.1_0.2'' ELSE ''0_0.1'' END AS MAXDEMAND_KG_ORAN,
      8 AS TAGTYPE
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' S,
      '||gv_stg_owner||'.'||v_src_table_02||' M,
      '||gv_edw_owner||'.'||v_src_table_03||' D
    WHERE 
      S.TESISAT_NO=M.TESISAT_NO AND 
      M.TESISAT_NO=D.SUBSCRIBER_ID(+) AND 
      D.INSTALLED_POWER(+)>0  
    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
    PROCEDURE PRC_ALLFIELD_CONTROLL
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'ALLFIELD_CONTROLL';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(60) := 'SKTS_TBLKCKTTOZET';
    v_src_table_02  varchar2(60) := 'SYS_WORK_ORDERS';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_ALLFIELD_CONTROLL';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    WITH SKTS AS  ----SKTSDEK� FORMLARIN T�M�N� ��EREN TABLO--                                                                                                                      
    (                                                                                                                                                            
    SELECT  
      FORMTARIHYIL*10000+FORMTARIHAY*100+EXTRACT(DAY FROM FORMTARIH) FORM_TARIH,
      TESISATNO,                                                                                                             
      SUM(CASE  WHEN FORMTIP=15 OR FORMTIP=33 THEN 1 ELSE 0 END) AS KACAKADET,      
      SUM(CASE  WHEN FORMTIP=13 THEN 1 ELSE 0 END) AS DENETIMADET, 
      SUM(CASE  WHEN FORMTIP=17 THEN 1 ELSE 0 END) AS SAYACDEGISIMADET    
    FROM      
       '||gv_ods_owner||'.'||v_src_table_01||'                                                                                                                      
    WHERE                                                                                                                                                
       REGEXP_LIKE(TESISATNO, ''^[[:digit:]]+$'')                                                                         
    GROUP BY
       TESISATNO,
       FORMTARIHYIL*10000+FORMTARIHAY*100+EXTRACT(DAY FROM FORMTARIH)                                                                                                 
    ), 
    SKTS_SYS AS --- SKTS VE SYS KONTROLLER�N�N B�RLE��M�--
    (                                                                                                                                            
    SELECT  
     FORM_TARIH,
     CAST(TESISATNO AS INT) TESISATNO ,
     ''SKTS'' KAYNAK,                                                                                                                      
    CASE  WHEN   KACAKADET>0        THEN  ''KACAK''                                                               
          WHEN   SAYACDEGISIMADET>0 THEN  ''SAYAC''                                                                               
          WHEN   DENETIMADET>0      THEN  ''DENETIM'' END KONTROLDURUM                                                  
    FROM  SKTS

    UNION ALL 

    SELECT 
      TO_NUMBER(TO_CHAR(WORK_ORDER_END_DATE,''YYYYMMDD''))  FORM_TARIH,
      CAST(SUBSCRIBER_ID AS INT) TESISATNO, 
      ''SYS'' KAYNAK,
      CASE WHEN LEAKAGE_DETECTION_RESULT_ID=''1'' THEN ''KACAK''
           WHEN LEAKAGE_DETECTION_RESULT_ID=''2'' THEN ''EKTAHAKKUK''
           WHEN LEAKAGE_DETECTION_RESULT_ID=''3'' AND INSTALLING_METER_REASON_ID=''1'' THEN ''ARIZA'' 
           ELSE ''DENETIM'' 
      END KONTROLDURUM
    FROM
      '||gv_stg_owner||'.'||v_src_table_02||'
    WHERE 
      SUBSCRIBER_ID>0 AND 
      WORK_ORDER_STATUS_ID=9 AND --TAMAMLANAN ��EMR�--
      WORK_ORDER_TYPE_ID=3   --ABONE KONTROL ��EMR�
    )
    --- SKTS VE SYS B�RLE��M TABLOSUNDAN KONTROLLER�N D�N��LER�YLE ALINDI�I TABLOLAR--
    SELECT 
      FORM_TARIH,
      TESISATNO,
      KONTROLDURUM,
      KAYNAK,
      ROW_NUMBER() OVER ( PARTITION BY TESISATNO ORDER BY FORM_TARIH DESC) RN
    FROM 
      SKTS_SYS 
    WHERE  
      TESISATNO>1000000
   ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  

    PROCEDURE PRC_TMP_DIYOT_TAG001
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_DIYOT_TAG001';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'VOLTAGE_ERROR_TAG001';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_DIYOT_TAG001';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      ROW_DATE,
      WIRINGNO TESISAT_NO,
      SCORE PUAN,
      FAULT_TYPE 
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' 
    WHERE 
      FAULT_TYPE=''D''
    GROUP BY
      ROW_DATE,
      WIRINGNO,
      SCORE,
      FAULT_TYPE
   ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
   PROCEDURE PRC_TMP_BOBIN_TAG002
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_BOBIN_TAG002';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'VOLTAGE_ERROR_TAG001';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_BOBIN_TAG002';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      ROW_DATE,
      WIRINGNO TESISAT_NO,
      SCORE PUAN,
      FAULT_TYPE
    FROM  '||gv_stg_owner||'.'||v_src_table_01||' 
    WHERE 
      FAULT_TYPE=''B''
    GROUP BY
      ROW_DATE,
      WIRINGNO,
      SCORE,
      FAULT_TYPE
      
   ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  PROCEDURE PRC_TMP_FLOW_TAG003
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMPFLOW_TAG003';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'FLOW_TAG002';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_FLOW_TAG003';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT
      ROW_DATE ,
      WIRINGNO TESISAT_NO,
      SCORE PUAN,
      ''AKIM'' FAULT_TYPE
    FROM  
      '||gv_stg_owner||'.'||v_src_table_01||' 
    GROUP BY 
      ROW_DATE,
      WIRINGNO,
      SCORE
    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  
  PROCEDURE PRC_TMP_COS_TAG004
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMPCOS_TAG004';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'COS_TAG003';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_COS_TAG004';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      ROW_DATE,
      WIRINGNO TESISAT_NO,
      SCORE  PUAN,
      ''COS'' FAULT_TYPE
    FROM  
      '||gv_stg_owner||'.'||v_src_table_01||' 
    GROUP BY 
      ROW_DATE,
      WIRINGNO,
      SCORE
   ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  PROCEDURE PRC_TMP_ENDEX_TAG005
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_ENDEX_TAG005';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'ENDEX_TAG004';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_ENDEX_TAG005';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      ROW_DATE,
      WIRINGNO TESISAT_NO,
      SCORE  PUAN,
      ''ENDEX'' FAULT_TYPE
    FROM  
      '||gv_stg_owner||'.'||v_src_table_01||' 
    GROUP BY 
      ROW_DATE,
      WIRINGNO,
      SCORE
      
    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  PROCEDURE PRC_TMP_KUMANDATAG006
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_KUMANDA006';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'COMMAND_TAG005';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_KUMANDATAG006';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT  
      ROW_DATE, 
      WIRINGNO TESISAT_NO,
      SCORE PUAN,
      ''KUMANDA'' FAULT_TYPE
    FROM  
      '||gv_stg_owner||'.'||v_src_table_01||'
    GROUP BY 
      ROW_DATE,
      WIRINGNO,
      SCORE
      

    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  
  
  PROCEDURE PRC_TMP_MBSKACAKTAG007
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_MBSKACAKTAG007';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'LEAKAGEINVOICE_TAG006';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_MBSKACAKTAG007';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      TESISAT_NO,
      KACAKTARIHI,
      KO_KWH,
      KSI_KWH,
      KSS_KWH,
      CASE WHEN SONFATURA_KO_BUYUKMU+KACAKSONRASI_ARTIS =0  
      AND KACAKTARIHI>20140100 THEN 5 ELSE 0 END KACAK_PUAN 
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||'    
    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  PROCEDURE PRC_TMP_SAYACTAG008
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_SAYACTAG008';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'METERINVOICE_TAG007';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_SAYACTAG008';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      TESISAT_NO,
      TAKILMA_TARIHI,
      SO_KWH ,
      SSI_KWH,
      SSS_KWH,
      CASE WHEN SAYACSONRASIARTISDUSUS=1 AND TAKILMA_TARIHI>20140100 THEN 5 ELSE
      0 END SAYAC_PUAN
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||'  
    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  
  PROCEDURE PRC_TMP_DEMANDTAG009
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_DEMANDTAG009';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'DEMAND_TAG008';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_DEMANDTAG009';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
     SELECT 
      TESISAT_NO,
      SONDEMAND,
      MAXDEMAND,
      CASE WHEN  SONDEMAN_MAXDEMANDORAN IN (''0.20_0.10'',''0.10_0.0'') THEN 3 ELSE 0 END DEMAND_PUAN
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' 
    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  
  
  PROCEDURE PRC_TMP_MAGNETICTAG0010(pid_start_date date default trunc(sysdate-31), pid_end_date date default trunc(sysdate-1))
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_MAGNETICTAG0010';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'ARIL_ARILADM_EVT_MTPLOGS';
    v_src_table_02  varchar2(30) := 'ARIL_ARILADM_EVT_TYPES';
    v_src_table_03  varchar2(30) := 'D_SUBSCRIBERS';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_MAGNETICTAG0010';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
     SELECT  
        S.SUBSCRIBER_ID TESISAT_NO,
        3 PUAN,
        ''MANYETIK''FAULT_TYPE,
        CASE WHEN SUBSTR(ENDDATE,1,8) IS NOT NULL THEN SUBSTR(ENDDATE,1,8) ELSE TO_CHAR(SYSDATE,''YYYYMMDD'') END ROW_DATE
     FROM 
        '||gv_ods_owner||'.'||v_src_table_01||' L,
        '||gv_ods_owner||'.'||v_src_table_02||' T,
        '||gv_edw_owner||'.'||v_src_table_03||' S
     WHERE 
       L.EVENTCODE=T."EventCode"(+) AND 
       TO_NUMBER(L.METERPOINTIDENTIFIER)= S.METER_NUMBER(+) AND 
        ('||to_char(pid_start_date,'YYYYMMDD')||'=SUBSTR(ENDDATE,1,8) or   '||to_char(pid_end_date,'YYYYMMDD')||'=SUBSTR(ENDDATE,1,8) ) AND
       ( ( TO_DATE(ENDDATE,''YYYYMMDDHH24miSS'')-TO_DATE(STARTDATE,''YYYYMMDDHH24miSS'') )*24*60>30 OR ( STARTDATE>0 AND ( ENDDATE IS NULL OR ENDDATE=0 ) ) )
       AND  --30 dakikadan fazla s�renler
       L.EVENTCODE=''M14''      
    GROUP BY
      S.SUBSCRIBER_ID,
      CASE WHEN SUBSTR(ENDDATE,1,8) IS NOT NULL THEN SUBSTR(ENDDATE,1,8) ELSE TO_CHAR(SYSDATE,''YYYYMMDD'') END
    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  
   PROCEDURE PRC_TMP_GOVDETAG011
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_GOVDETAG011';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'ARIL_ARILADM_EVT_MTPLOGS';
    v_src_table_02  varchar2(30) := 'ARIL_ARILADM_EVT_TYPES';
    v_src_table_03  varchar2(30) := 'D_SUBSCRIBERS';
    v_src_table_04  varchar2(30) := 'TMP_METER_TAG7';
    
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_GOVDETAG011';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT  
      S.SUBSCRIBER_ID TESISAT_NO,
      MIN( TO_NUMBER(SUBSTR(ENDDATE,1,8))) TARIH,
      5 PUAN,
      ''GOVDE'' FAULT_TYPE
    FROM 
      '||gv_ods_owner||'.'||v_src_table_01||' L,
      '||gv_ods_owner||'.'||v_src_table_02||' T,
      '||gv_edw_owner||'.'||v_src_table_03||' S,
      '||gv_stg_owner||'.'||v_src_table_04||' TAG7
    WHERE 
      L.EVENTCODE=T."EventCode"(+) AND 
      TO_NUMBER(L.METERPOINTIDENTIFIER)= S.METER_NUMBER AND
      S.SUBSCRIBER_ID=S.SUBSCRIBER_ID AND
      L.EVENTCODE=''M02'' AND
      TAG7.TESISAT_NO(+)=S.SUBSCRIBER_ID AND
      TAG7.TAKILMA_TARIHI<TO_NUMBER(SUBSTR(ENDDATE,1,8))
     GROUP BY
       S.SUBSCRIBER_ID
    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  PROCEDURE PRC_TMP_LASTACCESSTAG012
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_LASTACCESSTAG012';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(60) := 'AMR_ALL_DETAILS_ARIL';
  
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_GOVDETAG012';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      LAST_ACCESS_DATE,
      SUBSCRIBER_ID TESISAT_NO,
      CASE WHEN LAST_ACCESS_DATE<TRUNC(SYSDATE-30) THEN ''ERISIM YOK'' ELSE ''ERISIM VAR'' END ERISIMDURUM,
      SYSDATE-1 SUPHEGUN,
      ''3'' ERISIM_PUAN
    FROM 
      '||gv_edw_owner||'.'||v_src_table_01||'  
    WHERE 
      INSTALLED_POWER>15000 AND 
      LAST_ACCESS_DATE<TRUNC(SYSDATE-30)
    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;  
  
  
  
  PROCEDURE PRC_TMP_SUBSCRIBER_POOL
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_SUBSCRIBER_POOL';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'TMP_DIYOT_TAG001';
    v_src_table_02  varchar2(30) := 'TMP_BOBIN_TAG002';
    v_src_table_03  varchar2(30) := 'TMPFLOW_TAG003';
    v_src_table_04  varchar2(30) := 'TMPCOS_TAG004';
    v_src_table_05  varchar2(30) := 'TMP_ENDEX_TAG005';
    v_src_table_06  varchar2(30) := 'TMP_KUMANDA006';   
    v_src_table_07  varchar2(30) := 'TMP_MAGNETICTAG0010';
    v_src_table_08  varchar2(30) := 'TMP_GOVDETAG011';
    v_src_table_09  varchar2(30) := 'TMP_LASTACCESSTAG012';
      
    ---------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_SUBSCRIBER_POOL';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      TESISAT_NO,
      ROW_DATE
    FROM
      (
       SELECT
         A.ROW_DATE, 
         A.TESISAT_NO
       FROM 
         '||gv_stg_owner||'.'||v_src_table_01||' A
        
       UNION ALL
        
       SELECT
         B.ROW_DATE ,
         B.TESISAT_NO
       FROM 
         '||gv_stg_owner||'.'||v_src_table_02||' B
       
       UNION ALL
        
       SELECT
         C.ROW_DATE ,
         C.TESISAT_NO
       FROM 
         '||gv_stg_owner||'.'||v_src_table_03||' C
         
       UNION ALL
        
       SELECT
         D.ROW_DATE ,
         D.TESISAT_NO
       FROM 
         '||gv_stg_owner||'.'||v_src_table_04||' D
         
       UNION ALL
        
       SELECT
         E.ROW_DATE ,
         E.TESISAT_NO
       FROM 
         '||gv_stg_owner||'.'||v_src_table_05||' E
        
       UNION ALL
        
       SELECT
        F.ROW_DATE ,
        F.TESISAT_NO
       FROM 
        '||gv_stg_owner||'.'||v_src_table_06||' F
        
          UNION ALL
        
       SELECT
         TO_CHAR(SYSDATE-1,''YYYYMMDD'') ROW_DATE ,
         TO_CHAR(G.TESISAT_NO) TESISAT_NO
       FROM 
         '||gv_stg_owner||'.'||v_src_table_07||' G
        
       UNION ALL
        
       SELECT
        TO_CHAR(SYSDATE-1,''YYYYMMDD'') ROW_DATE ,
        TO_CHAR(H.TESISAT_NO) TESISAT_NO
       FROM 
        '||gv_stg_owner||'.'||v_src_table_08||' H 
        
        UNION ALL
        
       SELECT
        TO_CHAR(SYSDATE-1,''YYYYMMDD'') ROW_DATE ,
        TO_CHAR(I.TESISAT_NO) TESISAT_NO
       FROM 
        '||gv_stg_owner||'.'||v_src_table_09||' I         
        )
         GROUP BY
         TESISAT_NO,
         ROW_DATE
   ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  PROCEDURE PRC_TMP_SUBSCRIBER_POOL_LAST
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_SUBSCRIBER_POOL_LAST';
    ---------------------------------------------------------------------------- 
    v_src_table_01  varchar2(30) := 'TMP_DIYOT_TAG001';
    v_src_table_02  varchar2(30) := 'TMP_BOBIN_TAG002';
    v_src_table_03  varchar2(30) := 'TMPFLOW_TAG003';
    v_src_table_04  varchar2(30) := 'TMPCOS_TAG004';
    v_src_table_05  varchar2(30) := 'TMP_ENDEX_TAG005';
    v_src_table_06  varchar2(30) := 'TMP_KUMANDA006';  
    v_src_table_07  varchar2(30) := 'TMP_SUBSCRIBER_POOL';  
    v_src_table_08  varchar2(30) := 'TMP_MAGNETICTAG0010';
    v_src_table_09  varchar2(30) := 'TMP_GOVDETAG011'; 
    v_src_table_10  varchar2(30) := 'TMP_LASTACCESSTAG012';  
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_SUBSCRIBER_POOL_LAST';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT
    A.ROW_DATE,
    TO_NUMBER(A.TESISAT_NO) TESISAT_NO,
    B.PUAN DIYOT_PUAN,
    C.PUAN BOBIN_PUAN,
    D.PUAN AKIM_PUAN,
    E.PUAN COS_PUAN,
    F.PUAN ENDEX_PUAN,
    G.PUAN KUMANDA_PUAN,
    H.PUAN MANYETIK_PUAN,
    J.PUAN GOVDE_PUAN,
    K.ERISIM_PUAN
    FROM
      '||gv_stg_owner||'.'||v_src_table_07||' A,
      '||gv_stg_owner||'.'||v_src_table_01||' B,
      '||gv_stg_owner||'.'||v_src_table_02||' C,
      '||gv_stg_owner||'.'||v_src_table_03||' D,
      '||gv_stg_owner||'.'||v_src_table_04||' E,
      '||gv_stg_owner||'.'||v_src_table_05||' F,
      '||gv_stg_owner||'.'||v_src_table_06||' G,
      '||gv_stg_owner||'.'||v_src_table_08||' H,
      '||gv_stg_owner||'.'||v_src_table_09||' J,
      '||gv_stg_owner||'.'||v_src_table_10||' K
      
    WHERE 
        ( A.TESISAT_NO=B.TESISAT_NO(+) AND A.ROW_DATE=B.ROW_DATE(+) )
    AND ( A.TESISAT_NO=C.TESISAT_NO(+) AND A.ROW_DATE=C.ROW_DATE(+) )  
    AND ( A.TESISAT_NO=D.TESISAT_NO(+) AND A.ROW_DATE=D.ROW_DATE(+) )    
    AND ( A.TESISAT_NO=E.TESISAT_NO(+) AND A.ROW_DATE=E.ROW_DATE(+) )  
    AND ( A.TESISAT_NO=F.TESISAT_NO(+) AND A.ROW_DATE=F.ROW_DATE(+) )  
    AND ( A.TESISAT_NO=G.TESISAT_NO(+) AND A.ROW_DATE=G.ROW_DATE(+) ) 
    AND A.TESISAT_NO IS NOT NULL 
    AND ( A.TESISAT_NO=H.TESISAT_NO(+)) 
    AND ( A.TESISAT_NO=J.TESISAT_NO(+)) 
    AND ( A.TESISAT_NO=K.TESISAT_NO(+)) 
   ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  
  PROCEDURE  PRC_SUBSCRIBER_ANALYSIS_RECORD
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'SUBSCRIBER_ANALYSIS_RECORD';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'D_SUBSCRIBERS';
    v_src_table_02  varchar2(30) := 'TMP_MBSKACAKTAG007';
    v_src_table_03  varchar2(30) := 'TMP_SAYACTAG008';
    v_src_table_04  varchar2(30) := 'TMP_DEMANDTAG009';
    v_src_table_05  varchar2(30) := 'ALLFIELD_CONTROLL';
    v_src_table_06  varchar2(30) := 'TMP_SUBSCRIBER_POOL_LAST';
  ----------------------------------------------------------------------------

   BEGIN
  
    gv_proc := 'PRC_SUBSCRIBER_ANALYSIS_RECORD';
  
    -- Initialize Log Variables
    plib.o_log := log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);


    plib.truncate_table(gv_stg_owner, v_table_name);
    
    plib.enable_parallel_dml;
    
    gv_dyn_task := '
    INSERT /*+  APPEND NOLOGGING */ INTO '||gv_stg_owner ||'.'||v_table_name||'
    (
      TESISAT_NO ,     
      UNVAN      ,     
      IL             ,    
      ADRES          ,   
      BOLGE_ADI      ,  
      KURULU_GUC     , 
      ABONE_GRUP_ADI ,
      TARIFE_ADI     , 
      UNIPED_ADI     , 
      KESME_DURUM_ADI, 
      ENDEKS_DURUM_ADI,
      ABONE_DURUM_ADI, 
      OSOS_DURUMU    ,   
      OG_DURUMU      ,   
      ABONE_TURU     ,  
      ROW_DATE       ,
      DIYOT_PUAN     , 
      BOBIN_PUAN     , 
      AKIM_PUAN      , 
      COS_PUAN       , 
      ENDEX_PUAN     , 
      KUMANDA_PUAN   ,
      MANYETIK_PUAN  ,
      GOVDE_PUAN     , 
      ERISIM_PUAN,
      KACAKTARIHI    , 
      KO_KWH         ,
      KSI_KWH        , 
      KSS_KWH        , 
      KACAK_PUAN     , 
      TAKILMA_TARIHI , 
      SO_KWH         ,
      SSI_KWH        , 
      SSS_KWH        , 
      SAYAC_PUAN     , 
      SONDEMAND      , 
      MAXDEMAND      , 
      DEMAND_PUAN    , 
      KONTROL_TARIHI , 
      KONTROLDURUM   , 
      KAYNAK         , 
      KONTROL_AY     , 
      TOPLAMPUAN     , 
      KURALADET 
      )
     SELECT /*+ PARALLEL(D 16) PARALLEL(TAG1 16) PARALLEL(TAG1_1 16)  PARALLEL(TAG2 16)  PARALLEL(TAG3 16)  PARALLEL(TAG4 16)
     PARALLEL(TAG5 16)  PARALLEL(TAG6 16)  PARALLEL(TAG7 16)  PARALLEL(TAG8 16)  PARALLEL(KNT 16) */
      D.SUBSCRIBER_ID TESISAT_NO,
      D.NAME UNVAN,
      D.PROVINCE IL,
      D.ADDRESS ADRES, 
      D.AREA_NAME BOLGE_ADI, 
      D.INSTALLED_POWER KURULU_GUC, 
      D.SUBSCRIBER_GROUP_NAME ABONE_GRUP_ADI,
      D.TARIFF_NAME TARIFE_ADI,
      D.UNIPED_NAME UNIPED_ADI, 
      D.CUT_STATUS_NAME KESME_DURUM_ADI, 
      D.ENDEX_STATUS_NAME ENDEKS_DURUM_ADI,
      D.SUBSCRIBER_STATUS_NAME ABONE_DURUM_ADI,
      D.OSOS_STATUS OSOS_DURUMU, 
      D.OG_STATUS OG_DURUMU, 
      D.SUBSCRIBER_KIND ABONE_TURU, 
      --------------------------------------------------------------------------
      S.ROW_DATE, 
      S.DIYOT_PUAN,
      S.BOBIN_PUAN, 
      S.AKIM_PUAN, 
      S.COS_PUAN,
      S.ENDEX_PUAN, 
      S.KUMANDA_PUAN,
      S.MANYETIK_PUAN,
      S.GOVDE_PUAN,
      S.ERISIM_PUAN,
      
      -----KA�AK ��PHE----------------------------------------------------------
      MBSK.KACAKTARIHI,
      MBSK.KO_KWH,
      MBSK.KSI_KWH,
      MBSK.KSS_KWH,
      MBSK.KACAK_PUAN,
      
      -----SAYA� ��PHE----------------------------------------------------------
      MBSS.TAKILMA_TARIHI,
      MBSS.SO_KWH ,
      MBSS.SSI_KWH,
      MBSS.SSS_KWH,
      MBSS.SAYAC_PUAN,
      
      -----DEMAND ��PHE---------------------------------------------------------
      MBSD.SONDEMAND,
      MBSD.MAXDEMAND,
      MBSD.DEMAND_PUAN, 
      
      KNT.FORM_TARIH KONTROL_TARIHI,  
      KNT.KONTROLDURUM, 
      KNT.KAYNAK,
      SUBSTR(KNT.FORM_TARIH,1,6) KONTROL_AY,
      
      (  
        NVL(S.DIYOT_PUAN,0)+
        NVL(S.BOBIN_PUAN,0)+
        NVL(S.AKIM_PUAN,0)+
        NVL(S.COS_PUAN,0)+
        NVL(S.ENDEX_PUAN,0)+
        NVL(S.KUMANDA_PUAN,0)+
        NVL(MBSK.KACAK_PUAN,0)+
        NVL(MBSS.SAYAC_PUAN,0)+
        NVL(MBSD.DEMAND_PUAN,0)+
        NVL(S.MANYETIK_PUAN,0)+
        NVL(S.ERISIM_PUAN,0)
        
      ) TOPLAMPUAN, 
      (
        CASE WHEN S.DIYOT_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN S.BOBIN_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN S.AKIM_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN S.COS_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN S.ENDEX_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN S.KUMANDA_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN MBSK.KACAK_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN MBSS.SAYAC_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN MBSD.DEMAND_PUAN>0 THEN 1 ELSE 0 END +
        CASE WHEN S.MANYETIK_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN S.GOVDE_PUAN>0 THEN 1 ELSE 0 END +
        CASE WHEN S.ERISIM_PUAN>0 THEN 1 ELSE 0 END
      ) KURALADET
    FROM 
     
      '||gv_edw_owner||'.'||v_src_table_01||' D,
      '||gv_stg_owner||'.'||v_src_table_02||' MBSK,
      '||gv_stg_owner||'.'||v_src_table_03||' MBSS,
      '||gv_stg_owner||'.'||v_src_table_04||' MBSD,
      '||gv_stg_owner||'.'||v_src_table_06||' S,
      (
      SELECT * FROM  '||gv_stg_owner||'.'||v_src_table_05||' WHERE RN=1
      ) KNT
    WHERE 
        D.SUBSCRIBER_ID=MBSK.TESISAT_NO(+) AND 
        D.SUBSCRIBER_ID=MBSS.TESISAT_NO(+) AND 
        D.SUBSCRIBER_ID=MBSD.TESISAT_NO(+) AND
        D.SUBSCRIBER_ID=S.TESISAT_NO(+) AND 
        D.SUBSCRIBER_ID=KNT.TESISATNO(+)  AND 
        (
        CASE WHEN S.DIYOT_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN S.BOBIN_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN S.AKIM_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN S.COS_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN S.ENDEX_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN S.KUMANDA_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN S.MANYETIK_PUAN>0 THEN 1 ELSE 0 END+
        CASE WHEN S.GOVDE_PUAN>0 THEN 1 ELSE 0 END +
        CASE WHEN S.ERISIM_PUAN>0 THEN 1 ELSE 0 END
        
        )  >0
        AND      
        D.INSTALLED_POWER>15000  AND 
        SUBSCRIBER_GROUP_NAME NOT LIKE ''%RES%''  AND 
        D.UNIPED_NAME NOT LIKE ''%HAB%'' AND
        D.OSOS_STATUS=''Evet''
   ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  PROCEDURE PRC_PARTITION_ANALYSIS_RECORD 
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'PARTITION_ANALYSIS_RECORD';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'SUBSCRIBER_ANALYSIS_RECORD';
    ----------------------------------------------------------------------------
   
  
  BEGIN
    
    gv_proc := 'PRC_PARTITION_ANALYSIS_RECORD';

    plib.window_date_partitions_reeng(
       gv_dwh_owner,
       v_table_name,
       sysdate,
       gv_d_window_size
    );

      plib.truncate_partition(
        gv_dwh_owner,
        v_table_name,
        'P'||to_char(sysdate-31,'YYYYMMDD')
      );
      
       plib.truncate_partition(
        gv_dwh_owner,
        v_table_name,
        'P'||to_char(sysdate-1,'YYYYMMDD')
      );

      
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    gv_dyn_task := '
      INSERT /*+ append nologging */ INTO '||gv_dwh_owner||'.'||v_table_name||'
      (
       TESISAT_NO,
       UNVAN, 
       IL, 
       ADRES, 
       BOLGE_ADI, 
       KURULU_GUC, 
       ABONE_GRUP_ADI, 
       TARIFE_ADI, 
       UNIPED_ADI, 
       KESME_DURUM_ADI, 
       ENDEKS_DURUM_ADI,
       ABONE_DURUM_ADI, 
       OSOS_DURUMU, 
       OG_DURUMU, 
       ABONE_TURU, 
       ROW_DATE, 
       DIYOT_PUAN, 
       BOBIN_PUAN, 
       AKIM_PUAN, 
       COS_PUAN, 
       ENDEX_PUAN, 
       KUMANDA_PUAN, 
       MANYETIK_PUAN,
       GOVDE_PUAN,
       ERISIM_PUAN,
       KACAKTARIHI, 
       KO_KWH, 
       KSI_KWH, 
       KSS_KWH, 
       KACAK_PUAN, 
       TAKILMA_TARIHI, 
       SO_KWH, 
       SSI_KWH, 
       SSS_KWH, 
       SAYAC_PUAN,
       SONDEMAND, 
       MAXDEMAND, 
       DEMAND_PUAN, 
       KONTROL_TARIHI, 
       KONTROLDURUM, 
       KAYNAK, 
       KONTROL_AY, 
       TOPLAMPUAN, 
       KURALADET, 
       UPDATE_DATE
      )
        SELECT /*+parallel(H,8)*/
          TESISAT_NO,
          UNVAN,
          IL,
          ADRES,
          BOLGE_ADI,
          KURULU_GUC,
          ABONE_GRUP_ADI,
          TARIFE_ADI,
          UNIPED_ADI,
          KESME_DURUM_ADI,
          ENDEKS_DURUM_ADI,
          ABONE_DURUM_ADI,
          OSOS_DURUMU,
          OG_DURUMU,
          ABONE_TURU,
          TO_DATE(ROW_DATE,''YYYYMMDD'') ROW_DATE,
          DIYOT_PUAN,
          BOBIN_PUAN,
          AKIM_PUAN,
          COS_PUAN,
          ENDEX_PUAN,
          KUMANDA_PUAN,
          MANYETIK_PUAN,
          GOVDE_PUAN,
          ERISIM_PUAN,
          KACAKTARIHI,
          KO_KWH,
          KSI_KWH,
          KSS_KWH,
          KACAK_PUAN,
          TAKILMA_TARIHI,
          SO_KWH,
          SSI_KWH,
          SSS_KWH,
          SAYAC_PUAN,
          SONDEMAND,
          MAXDEMAND,
          DEMAND_PUAN,
          KONTROL_TARIHI,
          KONTROLDURUM,
          KAYNAK,
          KONTROL_AY,
          TOPLAMPUAN,
          KURALADET,
          TO_CHAR(SYSDATE,''YYYYMMDD'') UPDATE_DATE
        FROM 
          '||gv_stg_owner||'.'||v_src_table_01||' H

    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
   PROCEDURE PRC_TMP_SYSTEM_SUMMARY001
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_SYSTEM_SUMMARY001';
    ---------------------------------------------------------------------------- 
    v_src_table_01  varchar2(30) := 'PARTITION_ANALYSIS_RECORD';
    v_src_table_02  varchar2(30) := 'ALLFIELD_CONTROLL';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_SYSTEM_SUMMARY001';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      REC.TESISAT_NO,
      REC.ROW_DATE,
      SUM(CASE WHEN C.KONTROLDURUM=''KACAK'' THEN 1 ELSE 0 END)                 KACAK_ADET,
      SUM(CASE WHEN C.KONTROLDURUM=''EKTAHAKKUK'' THEN 1 ELSE 0 END)            EKTHK_ADET,
      SUM(CASE WHEN C.KONTROLDURUM=''ARIZA'' THEN 1 ELSE 0 END)                 ARIZA_ADET,
      SUM(CASE WHEN C.KONTROLDURUM=''SAYAC'' OR C.KONTROLDURUM=''DENETIM'' THEN 1 ELSE 0 END) DENETIM_ADET
    FROM
      '||gv_stg_owner||'.'||v_src_table_01||' REC,
      '||gv_stg_owner||'.'||v_src_table_02||' C
    WHERE 
      TO_NUMBER(REC.TESISAT_NO)=C.TESISATNO(+) AND 
      TO_DATE(C.FORM_TARIH(+),''YYYYMMDD'')>=REC.ROW_DATE AND
      TO_DATE(C.FORM_TARIH(+),''YYYYMMDD'')<REC.ROW_DATE+30
    GROUP BY 
      REC.TESISAT_NO,REC.ROW_DATE
   ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  PROCEDURE PRC_TMP_SYSTEM_SUMMARY002
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_SYSTEM_SUMMARY002';
    ---------------------------------------------------------------------------- 
    v_src_table_01  varchar2(30) := 'TMP_SYSTEM_SUMMARY001';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_SYSTEM_SUMMARY002';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      S.TESISAT_NO, 
      S.ROW_DATE, 
      S.KACAK_ADET, 
      S.EKTHK_ADET, 
      S.ARIZA_ADET, 
      S.DENETIM_ADET,
      CASE WHEN S.KACAK_ADET+S.EKTHK_ADET+S.ARIZA_ADET+S.DENETIM_ADET>0 THEN ''Sistem + Saha'' 
           WHEN S.KACAK_ADET+S.EKTHK_ADET+S.ARIZA_ADET+S.DENETIM_ADET=0 THEN ''Sistem'' END DONUS
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' S
   ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  PROCEDURE PRC_TMP_SYSTEM_SUMMARY003
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_SYSTEM_SUMMARY003';
    ---------------------------------------------------------------------------- 
    v_src_table_01  varchar2(30) := 'PARTITION_ANALYSIS_RECORD';
    v_src_table_02  varchar2(30) := 'ALLFIELD_CONTROLL';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_SYSTEM_SUMMARY003';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      C.TESISATNO TESISAT_NO,
      REC.ROW_DATE,
      SUM(CASE WHEN C.KONTROLDURUM=''KACAK'' THEN 1 ELSE 0 END)                 KACAK_ADET,
      SUM(CASE WHEN C.KONTROLDURUM=''EKTAHAKKUK'' THEN 1 ELSE 0 END)            EKTHK_ADET,
      SUM(CASE WHEN C.KONTROLDURUM=''ARIZA'' THEN 1 ELSE 0 END)                 ARIZA_ADET,
      SUM(CASE WHEN C.KONTROLDURUM=''SAYAC'' OR C.KONTROLDURUM=''DENETIM'' THEN 1 ELSE 0 END) DENETIM_ADET 
    FROM
      (SELECT ROW_DATE FROM '||gv_stg_owner||'.'||v_src_table_01||' GROUP BY ROW_DATE) REC,
      '||gv_stg_owner||'.'||v_src_table_02||' C
    WHERE 
      TO_DATE(C.FORM_TARIH,''YYYYMMDD'')>=REC.ROW_DATE AND
      TO_DATE(C.FORM_TARIH,''YYYYMMDD'')<REC.ROW_DATE+30
    GROUP BY 
      C.TESISATNO,
      ROW_DATE
   ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  PROCEDURE PRC_TMP_SYSTEM_SUMMARY004
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_SYSTEM_SUMMARY004';
    ---------------------------------------------------------------------------- 
    v_src_table_01  varchar2(30) := 'TMP_SYSTEM_SUMMARY003';
    v_src_table_02  varchar2(30) := 'TMP_SYSTEM_SUMMARY002';
    v_src_table_03  varchar2(30) := 'D_SUBSCRIBERS';
    
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_SYSTEM_SUMMARY004';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
     SELECT
      A.TESISAT_NO,
      A.ROW_DATE, 
      A.KACAK_ADET, 
      A.EKTHK_ADET, 
      A.ARIZA_ADET, 
      A.DENETIM_ADET,
      ''Saha'' DONUS
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' A,
      '||gv_stg_owner||'.'||v_src_table_02||' O,
      '||gv_EDW_owner||'.'||v_src_table_03||' S
    WHERE 
      A.TESISAT_NO=O.TESISAT_NO(+) AND 
      A.ROW_DATE=O.ROW_DATE(+) AND
      O.TESISAT_NO IS NULL AND
      S.INSTALLED_POWER>15000  AND 
      S.SUBSCRIBER_GROUP_NAME NOT LIKE ''%RES%''  AND 
      S.UNIPED_NAME NOT LIKE ''%HAB%'' AND
      S.SUBSCRIBER_ID(+)=A.TESISAT_NO
   ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END; 
  
  
  
    PROCEDURE PRC_TMP_SYSTEM_SUMMARY005
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_SYSTEM_SUMMARY005';
    ---------------------------------------------------------------------------- 
    v_src_table_01  varchar2(30) := 'TMP_SYSTEM_SUMMARY002';
    v_src_table_02  varchar2(30) := 'TMP_SYSTEM_SUMMARY004';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_SYSTEM_SUMMARY005';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      TESISAT_NO, 
      ROW_DATE, 
      KACAK_ADET, 
      EKTHK_ADET, 
      ARIZA_ADET, 
      DENETIM_ADET, 
      DONUS 
    FROM '||gv_stg_owner||'.'||v_src_table_01||'
    
    UNION
    
    SELECT 
      TESISAT_NO, 
      ROW_DATE, 
      KACAK_ADET, 
      EKTHK_ADET, 
      ARIZA_ADET, 
      DENETIM_ADET, 
      DONUS 
    FROM '||gv_stg_owner||'.'||v_src_table_02||' 
   ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END; 
  

  
  
  
   PROCEDURE PRC_SYSTEM_SUMMARY
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'SUBSCRIBER_SYSTEM_SUMMARY';
    ---------------------------------------------------------------------------- 
    v_src_table_01  varchar2(30) := 'TMP_SYSTEM_SUMMARY005';
    v_src_table_02  varchar2(30) := 'D_SUBSCRIBERS';
    v_src_table_03  varchar2(30) := 'MASTER_CBS';
    v_src_table_04  varchar2(35) := 'PARTITION_ANALYSIS_RECORD';
    v_src_table_05  varchar2(30) := 'ALLFIELD_CONTROLL';
    ----------------------------------------------------------------------------
   BEGIN
  
    gv_proc := 'PRC_SYSTEM_SUMMARY';
  
    -- Initialize Log Variables
    plib.o_log := log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.truncate_table(gv_stg_owner, v_table_name);
    
    plib.enable_parallel_dml;
    
    gv_dyn_task := '
    INSERT /*+  APPEND NOLOGGING */ INTO '||gv_stg_owner ||'.'||v_table_name||'
    (
    TESISAT_NO,
    KAYNAK, 
    SUPHEGUN, 
    CBS_X, 
    CBS_Y, 
    OG_DURUM, 
    IL, 
    BOLGE_ADI, 
    KURULU_GUC, 
    ABONE_GRUP_ADI, 
    KACAK_ADET, 
    EKTHK_ADET, 
    ARIZA_ADET, 
    DENETIM_ADET,
    SAHA_DONUS,
    TOPLAMPUAN,
    UNVAN,
    KONTROL_TARIHI
    )
    SELECT
      A.TESISAT_NO,
      A.DONUS KAYNAK,
      A.ROW_DATE SUPHEGUN,
      B.CBS_X,
      B.CBS_Y,
      D.OG_STATUS OG_DURUM,
      D.PROVINCE IL,
      D.AREA_NAME BOLGE_ADI,
      D.INSTALLED_POWER KURULU_GUC,
      D.SUBSCRIBER_GROUP_NAME ABONE_GRUP_ADI,
      CASE WHEN KACAK_ADET>0 THEN 1 ELSE 0 END KACAK_ADET,
      CASE WHEN KACAK_ADET=0 AND EKTHK_ADET>0 THEN 1 ELSE 0 END EKTHK_ADET,
      CASE WHEN KACAK_ADET=0 AND EKTHK_ADET=0 AND ARIZA_ADET>0 THEN 1 ELSE 0 END ARIZA_ADET,
      CASE WHEN KACAK_ADET=0 AND EKTHK_ADET=0 AND ARIZA_ADET=0 AND DENETIM_ADET>0 THEN 1 ELSE 0 END DENETIM_ADET,
      CASE 
        WHEN KACAK_ADET>0  THEN ''KACAK''
        WHEN EKTHK_ADET>0 THEN  ''EKTHK'' 
        WHEN ARIZA_ADET>0 THEN  ''ARIZA''
        WHEN DENETIM_ADET>0 THEN ''DENETIM''
        ELSE ''KONTROL YOK'' END SAHA_DONUS,
      C.TOPLAMPUAN,
      D.NAME UNVAN,
      KNT.FORM_TARIH KONTROL_TARIHI
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' A ,
      '||gv_edw_owner||'.'||v_src_table_02||' D,
      '||gv_ods_mbs_owner||'.'||v_src_table_03||' B,
      '||gv_stg_owner||'.'||v_src_table_04||' C,
      (
      SELECT * FROM  '||gv_stg_owner||'.'||v_src_table_05||' WHERE RN=1
      ) KNT
   WHERE 
     D.SUBSCRIBER_ID(+)=A.TESISAT_NO AND 
     A.TESISAT_NO=B.TESISAT_NO(+) AND 
     ( C.TESISAT_NO(+)=A.TESISAT_NO AND C.ROW_DATE(+)=A.ROW_DATE ) AND 
     A.TESISAT_NO=KNT.TESISATNO(+) 
     AND D.OSOS_STATUS=''Evet''
    ';
     
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
    PROCEDURE PRC_TMP_FOLLOW_SCREEN001
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_FOLLOW_SCREEN001';
    ---------------------------------------------------------------------------- 
    v_src_table_01  varchar2(30) := 'SUBSCRIBER_SYSTEM_SUMMARY';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_FOLLOW_SCREEN001';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      TESISAT_NO,
      ABONE_GRUP_ADI,
      BOLGE_ADI,
      IL,
      OG_DURUM,
      TO_CHAR(SUPHEGUN,''YYYYMM'') SUPHEAY, 
      SUM(KACAK_ADET) KACAKADET,
      SUM(EKTHK_ADET) EKTHKADET,
      SUM(ARIZA_ADET) ARIZAADET,
      SUM(DENETIM_ADET) DENETIMADET,
      MAX(TOPLAMPUAN) AS TOPLAMPUAN
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' 
    WHERE KAYNAK!=''Saha'' 
    GROUP BY 
      TESISAT_NO,
      TO_CHAR(SUPHEGUN,''YYYYMM''),
      ABONE_GRUP_ADI,
      BOLGE_ADI,
      IL,
      OG_DURUM
    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END; 
  
  
   PROCEDURE PRC_TMP_FOLLOW_SCREEN002
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_FOLLOW_SCREEN002';
    ---------------------------------------------------------------------------- 
    v_src_table_01  varchar2(30) := 'TMP_FOLLOW_SCREEN001';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_FOLLOW_SCREEN002';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      TESISAT_NO,
      SUPHEAY,
      ABONE_GRUP_ADI,
      BOLGE_ADI,
      IL,
      OG_DURUM,
      TOPLAMPUAN,
      Sum(CASE WHEN (KACAKADET+EKTHKADET+ARIZAADET)>0 THEN 1 ELSE 0 END)  SAHADONUS 
    FROM 
       '||gv_stg_owner||'.'||v_src_table_01||' 
    WHERE 
      KACAKADET>0 OR EKTHKADET>0 OR ARIZAADET>0 OR DENETIMADET>0 
    GROUP BY TESISAT_NO,SUPHEAY,ABONE_GRUP_ADI,
      BOLGE_ADI,
      IL,
      OG_DURUM,TOPLAMPUAN
    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END; 
  
  
  
  PROCEDURE PRC_TMP_FOLLOW_SCREEN003
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TMP_FOLLOW_SCREEN003';
    ---------------------------------------------------------------------------- 
    v_src_table_01  varchar2(30) := 'TMP_FOLLOW_SCREEN002';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_FOLLOW_SCREEN003';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
      TESISAT_NO,
      ABONE_GRUP_ADI,
      BOLGE_ADI,
      IL,
      OG_DURUM,SUPHEAY,
      TOPLAMPUAN,
      CASE WHEN SAHADONUS>0 THEN ''KACAK'' ELSE ''TEMIZ'' END SAHADONUS 
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' 
    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END; 
  
  
  
  PROCEDURE PRC_FOLLOW_SCREEN
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'FOLLOW_SCREEN';
    ---------------------------------------------------------------------------- 
    v_src_table_01  varchar2(30) := 'TMP_FOLLOW_SCREEN003';
    ----------------------------------------------------------------------------
   BEGIN
  
    gv_proc := 'PRC_FOLLOW_SCREEN';
  
    -- Initialize Log Variables
    plib.o_log := log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.truncate_table(gv_stg_owner, v_table_name);
    
    plib.enable_parallel_dml;
    
    gv_dyn_task := '
    INSERT /*+  APPEND NOLOGGING */ INTO '||gv_stg_owner ||'.'||v_table_name||'
    (
    SUPHEAY,
    SAHADONUS,
    ABONE_GRUP_ADI,
    BOLGE_ADI,
    IL,
    OG_DURUM,
    TOPLAMPUAN,
    TESISAT_NO
   )
    SELECT 
      SUPHEAY,
      SAHADONUS,
      ABONE_GRUP_ADI,
      BOLGE_ADI,
      IL,
      OG_DURUM,
      TOPLAMPUAN,
      TESISAT_NO
    FROM 
      '||gv_stg_owner||'.'||v_src_table_01||' 
    GROUP BY 
      SUPHEAY,
      SAHADONUS,
      ABONE_GRUP_ADI,
      BOLGE_ADI,
      IL,
      OG_DURUM ,
      TOPLAMPUAN,
      TESISAT_NO
    ';
     
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
    PROCEDURE PRC_TMP_ANALYSIS_MONTLY
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'SUBSCRIBER_ANALYSIS_MONTLY';
    ---------------------------------------------------------------------------- 
    v_src_table_01  varchar2(30) := 'PARTITION_ANALYSIS_RECORD';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TP_ANALYSIS_MONTLY';
     
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL NOLOGGING COMPRESS
    AS
    SELECT 
     TESISAT_NO,
     SUM(DIYOT_PUAN) DIYOTPUAN,
     SUM(BOBIN_PUAN) BOBINPUAN,
     SUM(AKIM_PUAN) AKIMPUAN,
     SUM(COS_PUAN) COSPUAN, 
     SUM(ENDEX_PUAN) ENDEXPUAN,
     SUM(KUMANDA_PUAN) KUMANDAPUAN,
     SUM(MANYETIK_PUAN) MANYETIKPUAN, 
     SUM(GOVDE_PUAN) GOVDEPUAN, 
     SUM(ERISIM_PUAN) ERISIMPUAN,
     COUNT(*) SUPHEADET,
     MAX(ROW_DATE) SONSUPHEGUN,
     MIN(ROW_DATE) ILKSUPHEGUN
    FROM 
     '||gv_stg_owner||'.'||v_src_table_01||' 
    GROUP BY 
      TESISAT_NO   
    ';
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END; 
  
  
  PROCEDURE PRC_SUBSCRIBER_ALL_RECORD
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'SUBSCRIBER_ALL_RECORD';
    ---------------------------------------------------------------------------- 
    v_src_table_01  varchar2(30) := 'D_SUBSCRIBERS';
    v_src_table_02  varchar2(30) := 'SUBSCRIBER_ANALYSIS_MONTLY';
    v_src_table_03  varchar2(30) := 'TMP_MBSKACAKTAG007';
    v_src_table_04  varchar2(30) := 'TMP_SAYACTAG008';
    v_src_table_05  varchar2(30) := 'TMP_DEMANDTAG009';
    v_src_table_06  varchar2(30) := 'ALLFIELD_CONTROLL';
    v_src_table_07  varchar2(30) := 'SAYAC';
    v_src_table_08  varchar2(30) := 'MASTER';
          
    ----------------------------------------------------------------------------
   BEGIN
  
    gv_proc := 'PRC_SUBSCRIBER_ALL_RECORD';
  
    -- Initialize Log Variables
    plib.o_log := log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.truncate_table(gv_stg_owner, v_table_name);
    
    plib.enable_parallel_dml;
    
    gv_dyn_task := '
    INSERT /*+  APPEND NOLOGGING */ INTO '||gv_stg_owner ||'.'||v_table_name||'
    ( 
    TESISAT_NO, 
    UNVAN, 
    IL, 
    BOLGE_ADI, 
    ADRES, 
    KURULU_GUC, 
    SAYAC_NO, 
    SAYAC_MARKA, 
    ABONE_GRUP_ADI, 
    ABONE_DURUM_ADI, 
    TARIFE_ADI, 
    ENDEKS_DURUM_ADI, 
    CBS_X, 
    CBS_Y, 
    OSOS_DURUM, 
    OG_DURUM, 
    KARNE_NO, 
    SAYAC_CARPAN, 
    DIYOTPUAN, 
    BOBINPUAN, 
    AKIMPUAN, 
    COSPUAN, 
    ENDEXPUAN, 
    KUMANDAPUAN, 
    MANYETIKPUAN, 
    GOVDEPUAN, 
    ERISIMPUAN, 
    SUPHEADET, 
    SONSUPHEGUN, 
    ILKSUPHEGUN, 
    KACAKTARIHI, 
    KO_KWH, 
    KSI_KWH, 
    KSS_KWH, 
    KACAK_PUAN, 
    TAKILMA_TARIHI, 
    SO_KWH, 
    SSI_KWH, 
    SSS_KWH, 
    SAYAC_PUAN, 
    SONDEMAND, 
    MAXDEMAND, 
    DEMAND_PUAN, 
    SONKONTROL, 
    TESISATNO, 
    KONTROLDURUM, 
    KAYNAK, 
    SAYAC_DURUM, 
    FAZ_SAYISI, 
    TOPLAM_PUAN
    )
    SELECT 
      D.SUBSCRIBER_ID TESISAT_NO,
      D.NAME UNVAN,
      D.PROVINCE IL,
      D.AREA_NAME BOLGE_ADI,
      D.ADDRESS ADRES,
      D.INSTALLED_POWER KURULU_GUC,
      D.METER_NUMBER SAYAC_NO,
      D.METER_MODEL SAYAC_MARKA,
      D.SUBSCRIBER_GROUP_NAME ABONE_GRUP_ADI,
      D.SUBSCRIBER_STATUS_NAME ABONE_DURUM_ADI,
      D.TARIFF_NAME TARIFE_ADI,
      D.ENDEX_STATUS_NAME ENDEKS_DURUM_ADI,
      D.LATITUDE CBS_X,
      D.LONGITUDE CBS_Y,
      D.OSOS_STATUS OSOS_DURUM,
      D.OG_STATUS OG_DURUM,
      D.RATION_CARD_ID KARNE_NO,
      D.METER_MULTIPLIER SAYAC_CARPAN,
      S.DIYOTPUAN, 
      S.BOBINPUAN, 
      S.AKIMPUAN, 
      S.COSPUAN, 
      S.ENDEXPUAN, 
      S.KUMANDAPUAN, 
      S.MANYETIKPUAN, 
      S.GOVDEPUAN, 
      S.ERISIMPUAN, 
      S.SUPHEADET, 
      S.SONSUPHEGUN, 
      S.ILKSUPHEGUN,
      K.KACAKTARIHI, 
      K.KO_KWH,  
      K.KSI_KWH,  
      K.KSS_KWH,
      K.KACAK_PUAN,
      M.TAKILMA_TARIHI, 
      M.SO_KWH, 
      M.SSI_KWH,
      M.SSS_KWH,
      M.SAYAC_PUAN,
      A.SONDEMAND,
      A.MAXDEMAND, 
      A.DEMAND_PUAN,
      B.FORM_TARIH SONKONTROL,
      B.TESISATNO,
      B.KONTROLDURUM,
      B.KAYNAK,
      CASE WHEN G.SAYAC_CINSI=1 THEN ''Elektronik'' 
           WHEN G.SAYAC_CINSI=2 THEN ''Kombi''
           WHEN G.SAYAC_CINSI=0 THEN ''Mekanik''
           ELSE ''Diger'' END SAYAC_DURUM,
       C.FAZ_SAYISI,
      NVL(S.BOBINPUAN,0)+NVL(S.AKIMPUAN,0)+NVL(S.COSPUAN,0)+NVL(S.ENDEXPUAN,0)+NVL(S.KUMANDAPUAN,0)+NVL(S.MANYETIKPUAN ,0)+NVL(S.GOVDEPUAN,0)+NVL(S.ERISIMPUAN,0)+NVL(K.KACAK_PUAN,0)+NVL(M.SAYAC_PUAN,0)+NVL(A.DEMAND_PUAN,0) TOPLAM_PUAN
    FROM 
     '||gv_edw_owner||'.'||v_src_table_01||' D,
     '||gv_stg_owner||'.'||v_src_table_02||'  S,
     '||gv_stg_owner||'.'||v_src_table_03||'  K,
     '||gv_stg_owner||'.'||v_src_table_04||'  M,
     '||gv_stg_owner||'.'||v_src_table_05||'  A,
      (SELECT FORM_TARIH,TESISATNO,KONTROLDURUM,KAYNAK FROM '||gv_stg_owner||'.'||v_src_table_06||'  WHERE RN=1) B,
      '||gv_ods_mbs_owner||'.'||v_src_table_07||' G,
      '||gv_ods_mbs_owner||'.'||v_src_table_08||' C
    WHERE 
       D.SUBSCRIBER_ID=S.TESISAT_NO(+) AND
       D.SUBSCRIBER_ID=K.TESISAT_NO(+) AND
       D.SUBSCRIBER_ID=M.TESISAT_NO(+) AND
       D.SUBSCRIBER_ID=A.TESISAT_NO(+) AND
       D.SUBSCRIBER_ID=B.TESISATNO(+)  AND 
       D.SUBSCRIBER_ID=G.TESISAT_NO(+) AND
       D.SUBSCRIBER_ID=C.TESISAT_NO(+) AND G.SAYAC_KODU(+)=1
  
    ';
     
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  
  
  end;
/