create or replace PACKAGE BODY                                PHYSICIANS_CLEAN_UP_PKG AS

	g_process_name                VARCHAR2(25):='PHYSICIAN_CLEAN_UP';

-- +---------------------------------------------------------------------------+
-- | Name: send_email                                                          |
-- | Description: Send the mail with physicians without NPI                    |
-- |                                                                           |
-- | Parameters:                                                               |
-- |     p_start_date,p_end_date                                               |
-- |                                                                           |
-- |                                                                           |
-- +---------------------------------------------------------------------------+
   PROCEDURE send_email(p_start_date DATE, p_end_date DATE)
      IS
      v_date_to_loop   DATE:=NULL;
   BEGIN
      IF thot.nbd_pkg.isholiday(trunc(p_start_date)) != 'H' THEN
         IF regexp_instr(to_char(p_start_date,'DAY'),('MONDAY'),1,1) > 0 THEN
            IF v_date_to_loop IS NULL AND thot.nbd_pkg.isholiday(trunc(p_start_date-3)) = 'H' THEN
               v_date_to_loop := p_start_date-3;
               WHILE thot.nbd_pkg.isholiday(trunc(v_date_to_loop))= 'H'
                  LOOP
                     v_date_to_loop := v_date_to_loop - 1;
                  END LOOP;
            ELSE
               v_date_to_loop := p_start_date-3;
            END IF;
            IF to_char(sysdate,'HH24:MI') = '08:00' THEN
               thot.PHYSICIANS_CLEAN_UP_PKG.send_email_physician_exception(trunc(v_date_to_loop),SYSDATE);
            ELSE
               thot.PHYSICIANS_CLEAN_UP_PKG.send_email_physician_exception(SYSDATE-1/24,SYSDATE);
            END IF;
         ELSE
            if v_date_to_loop is null and thot.nbd_pkg.isholiday(trunc(p_start_date-1)) = 'H' THEN
               v_date_to_loop := p_start_date-1;
               WHILE thot.nbd_pkg.isholiday(trunc(v_date_to_loop)) IN ('H','W')
               LOOP
                  v_date_to_loop := v_date_to_loop - 1;
               END LOOP;
            ELSE
               v_date_to_loop := p_start_date-2;
            END IF;

            IF to_char(sysdate,'HH24:MI') = '00:00' THEN
               thot.PHYSICIANS_CLEAN_UP_PKG.send_email_physician_exception(trunc(SYSDATE),SYSDATE);
            ELSE
               thot.PHYSICIANS_CLEAN_UP_PKG.send_email_physician_exception(SYSDATE-1/24,SYSDATE);
            END IF;
         END IF;
      END IF;
   COMMIT;
EXCEPTION WHEN OTHERS THEN
    save_msg(1
                     ,LABEL_ERROR
                     ,SQLERRM|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.send_email'
                     ,NULL --NPI_ID
                     ,NULL --PHYSICIAN_ID
                     ,NULL --DEA_ID
							,NULL --AIMI_HDR_ID
                     ,g_process_name);

   ROLLBACK;
END;

-- +---------------------------------------------------------------------------+
-- | Name: get_user_name                                                       |
-- | Description: Get the name for the XUSER on User's table .                 |
-- |                                                                           |
-- | Parameters:                                                               |
-- |     user = USER.XUSER                                                     |
-- | Return:                                                                   |
-- |     USER.NAME                                                             |
-- +---------------------------------------------------------------------------+
      FUNCTION get_user_name(p_user THOT.users.xuser%TYPE)
                              return VARCHAR2 IS

         v_user_name          THOT.users.xuser%TYPE;
      BEGIN

            IF(p_user IS NOT NULL)THEN

               SELECT user_name
                 INTO v_user_name
                 FROM THOT.users
                WHERE xuser = p_user;

               RETURN v_user_name;
            ELSE
               RETURN 'USER NOT FOUND';
            END IF;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 'USER NOT FOUND';
         WHEN OTHERS THEN
            save_msg(1
                     ,LABEL_ERROR
                     ,SQLERRM|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.get_user_name'
                     ,NULL --NPI_ID
                     ,NULL --PHYSICIAN_ID
                     ,NULL --DEA_ID
							,NULL --AIMI_HDR_ID
                     ,g_process_name);

            RETURN v_user_name;

      END;


-- +---------------------------------------------------------------------------+
-- | Name: send_email_physician_exception                                      |
-- | Description: Consult THOT.MDM_ENRICHMENT_EXCEPTIONS to retrieve data      |
-- |                between the parameters p_start_date and p_end_date         |
-- | Parameters:                                                               |
-- |     p_start_date                                                          |
-- |     p_end_date                                                            |
-- |                                                                           |
-- |                                                                           |
-- |                                                                           |
-- +---------------------------------------------------------------------------+

   PROCEDURE send_email_physician_exception(p_start_date DATE
                                            ,p_end_date DATE)IS

      CURSOR cur_physicians_exceptions IS
         SELECT ex.physician_id
               ,ex.last
               ,ex.first
               ,ex.previous_npi
               ,ex.action
               ,ex.created_by
               ,ex.creation_date
           FROM THOT.PHYSICIANS_NPI_EXCEPTIONS ex, THOT.PHYSICIANS p
          WHERE p.id              =  ex.PHYSICIAN_ID
            AND ex.creation_date >=  p_start_date
            AND ex.creation_date <=  p_end_date;

      TYPE r_physician_object      IS RECORD (
         physician_id              THOT.PHYSICIANS_NPI_EXCEPTIONS.physician_id%TYPE
         ,last                     THOT.PHYSICIANS_NPI_EXCEPTIONS.last%TYPE
         ,first                    THOT.PHYSICIANS_NPI_EXCEPTIONS.first%TYPE
         ,previous_npi             THOT.PHYSICIANS_NPI_EXCEPTIONS.previous_npi%TYPE
         ,action                   THOT.PHYSICIANS_NPI_EXCEPTIONS.action%TYPE
         ,created_by               THOT.PHYSICIANS_NPI_EXCEPTIONS.created_by%TYPE
         ,creation_date            THOT.PHYSICIANS_NPI_EXCEPTIONS.creation_date%TYPE
      );
      r_physician_exception        r_physician_object;
      v_html_table                 CLOB;
      v_html_body                  CLOB;
      v_temp_table                 CLOB;
      v_style                      VARCHAR2(300);
      v_distribution_list          THOT.FT_LOOKUPS.attribute1%TYPE;
      v_make_table                 VARCHAR2(1):='N';
      v_app_parameter              VARCHAR2(1):='N';
    BEGIN

      SELECT attribute1
        INTO v_app_parameter
      FROM THOT.FT_LOOKUPS
      WHERE type = 'PRT_PHYSICIANS_NPI_EXCEPTIONS';

      IF (v_app_parameter = 'Y') THEN

         SELECT attribute1
           INTO v_distribution_list
           FROM THOT.FT_LOOKUPS
          WHERE type = 'PRT_E_DISTRIBUTION_LIST';

         OPEN cur_physicians_exceptions;
            LOOP
               FETCH cur_physicians_exceptions INTO r_physician_exception;
               EXIT WHEN cur_physicians_exceptions%NOTFOUND;
               IF cur_physicians_exceptions%FOUND THEN
                  v_make_table := 'Y';
                  v_temp_table := v_temp_table || '<tr><td>' ||r_physician_exception.physician_id || '</td>';
                  v_temp_table := v_temp_table || '<td>' ||r_physician_exception.last || '</td>';
                  v_temp_table := v_temp_table || '<td>' ||r_physician_exception.first || '</td>';
                  v_temp_table := v_temp_table || '<td>' ||r_physician_exception.action || '</td>';
                  v_temp_table := v_temp_table || '<td>' ||r_physician_exception.previous_npi || '</td>';
                  v_temp_table := v_temp_table || '<td></td>';
                  v_temp_table := v_temp_table || '<td>' ||r_physician_exception.created_by || '</td>';
                  v_temp_table := v_temp_table || '<td>' ||get_user_name(r_physician_exception.created_by) || '</td>';
                  v_temp_table := v_temp_table || '<td>' ||to_date(r_physician_exception.creation_date,'yyyy/mm/dd HH24:MI:SS') || '</td></tr>';


               END IF;
            END LOOP;
         IF(v_make_table = 'Y')THEN
            v_html_table := '<table><caption><b>Physicians</b></caption><tr bgcolor="#E0E0E0";><th>Physician_Id</th><th>Last</th><th>First</th>' ;
            v_html_table := v_html_table  || '<th>Action</th><th>Previous Value</th><th>Current Value</th><th>User</th><th>User Name</th><th>Date</th></tr>';
            v_html_table := v_html_table || v_temp_table;
            v_style := '<style>table, th, td {border: 1px solid black;    border-collapse: collapse;}';
            v_style := v_style ||' th { padding: 5px;  text-align: center; font-size:10pt; }';
            v_style := v_style ||' td { padding: 5px;  text-align: left; font-size:8pt; background-color: #F0F0F0;}</style>';

            v_html_table :=v_html_table || '</table>';
            v_html_body  :='<html><head>' || v_style ||'<title>Title of the document</title></head><body>';
            v_html_body  := v_html_body ||'<p>Please find below the list of physicians created without NPI or where the NPI was updated to NULL.</p>';
            v_html_body  := v_html_body || v_html_table ;
            v_html_body  := v_html_body || '<p></p><p>Please do not reply to email, automated email.</p></body></html>';

            Thot.Email_Files_Pkg.Send_Email(pFrom =>'RxHomeSupport@AccredoHealth.com',
                                      pTo => v_distribution_list,
                                      pCc => NULL,
                                      pSubject => 'NEW/UPDATED Record w/o NPI.',
                                      pTextMsg => null,
                                      pHtmlMsg => v_html_body,
                                      pAttachment1 => NULL,
                                      pFilename1 => NULL,
                                      pAttachType1 => NULL,
                                      pAttachment2 => NULL,
                                      pFilename2 => NULL,
                                      pAttachType2 => NULL,
                                      pServerNum => 1);
         ELSE
            v_html_body  :='<html><head><title>Title of the document</title></head><body><p>No Physicians exceptions</p></body></html>';
         END IF;
      END IF;
      IF(cur_physicians_exceptions%ISOPEN)THEN
         CLOSE cur_physicians_exceptions;
      END IF;

   EXCEPTION
      WHEN OTHERS THEN
         save_msg(1
                  ,LABEL_ERROR
                  ,SQLERRM|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.send_email_physician_exception'
                  ,NULL --NPI_ID
                  ,NULL --PHYSICIAN_ID
                  ,NULL --DEA_ID
						,NULL --AIMI_HDR_ID
                  ,g_process_name);
   END;




-- +---------------------------------------------------------------------------+
-- | Name: save_msg                                                            |
-- | Description: Insert on FT_MESSAGE_LOG the messages from this package      |
-- |                                                                           |
-- | Parameters                                                                |
-- |     p_source_id                                                           |
-- |     p_msg_type                                                            |
-- |     p_message                                                             |
-- |     p_npi_id                                                              |
-- |     p_physician_id                                                        |
-- |     p_physician_dea_id                                                    |
-- |     p_aimi_header_id 																		 |
-- |     p_invoke_process                                                      |
-- +---------------------------------------------------------------------------+
    PROCEDURE save_msg( p_source_id          NUMBER   DEFAULT 1
                       ,p_msg_type           VARCHAR2 DEFAULT 'INFO'
                       ,p_message            VARCHAR2
                       ,p_npi_id             THOT.PHYSICIANS.NPI_ID%TYPE
                       ,p_physician_id       THOT.PHYSICIANS.ID%TYPE
                       ,p_physician_dea_id   THOT.PHYSICIANS.DEA_ID%TYPE
							  ,p_aimi_header_id     RXH_CUSTOM.web_services_trans.ID%TYPE
                       ,p_invoke_process     VARCHAR2)  IS

   PRAGMA AUTONOMOUS_TRANSACTION;
      save_transaction      			BOOLEAN :=FALSE;
      v_dummy               			VARCHAR2(100):=NULL;
		v_save_error_flag             VARCHAR2(1):='Y';
		v_save_warning_flag           VARCHAR2(1):='Y';
		v_save_info_flag              VARCHAR2(1):='Y';
		v_count  							NUMBER:=0;

   BEGIN
      save_transaction:=FALSE;

		SELECT NVL(MAX(attribute1),'Y')
		  INTO v_save_error_flag
		  FROM THOT.FT_LOOKUPS
       WHERE TYPE = 'PHYSICIANS_CLEAN_UP'
         AND CODE = 'ERROR_LOG_LEVEL'
			AND STATUS='A';

		SELECT NVL(MAX(attribute1),'Y')
		  INTO v_save_warning_flag
		  FROM THOT.FT_LOOKUPS
       WHERE TYPE = 'PHYSICIANS_CLEAN_UP'
         AND CODE = 'WARNING_LOG_LEVEL'
			AND STATUS='A';

		SELECT NVL(MAX(attribute1),'Y')
		  INTO v_save_info_flag
		  FROM THOT.FT_LOOKUPS
       WHERE TYPE = 'PHYSICIANS_CLEAN_UP'
         AND CODE = 'INFO_LOG_LEVEL'
			AND STATUS='A';

      IF(v_save_error_flag='Y' AND p_msg_type =LABEL_ERROR)THEN
         save_transaction :=TRUE;
      END IF;
      IF(v_save_warning_flag='Y' AND p_msg_type =LABEL_WARNING)THEN
         save_transaction :=TRUE;
      END IF;
      IF(v_save_info_flag='Y' AND p_msg_type =LABEL_INFO)THEN
         save_transaction :=TRUE;
      END IF;

      IF(save_transaction=TRUE)THEN
         thot.Ft_Message_Log_Pkg.insert_row (p_context            => 'PHYS_MDM_ENRICHMENT'
                                            ,p_source_process_id  => p_source_id      --1
                                            ,p_msg_type           => p_msg_type       --INFO,WARNING OR ERROR
                                            ,p_message            => '['||TO_CHAR(SYSDATE,'HH:MI:SS'||'] ') || p_message
                                            ,p_attr1              => p_npi_id
                                            ,p_attr2              => p_physician_id
                                            ,p_attr3              => p_physician_dea_id
                                            ,p_attr4              => p_aimi_header_id
                                            ,p_attr5              => v_dummy
                                            ,p_attr6              => v_dummy
                                            ,p_attr7              => v_dummy
                                            ,p_attr8              => v_dummy
                                            ,p_attr9              => v_dummy
                                            ,p_attr10             => g_process_name
                                            );
             COMMIT;
      END IF;

   EXCEPTION
      WHEN OTHERS THEN
         NULL;
   END;




-- +---------------------------------------------------------------------------+
-- | Name: insert_physician                                                    |
-- | Description: Insert on the staging table the new physician received from  |
-- |      the MDM service.  This method is called from ORACLE RXHOME directly  |
-- |     CQ27045                                                               |
-- | Parameters:                                                               |
-- |                                                                           |
-- |     p_npi_id     = physician npi                                          |
-- |     p_physician_id= Id of the new Physician                               |
-- +---------------------------------------------------------------------------+
   PROCEDURE insert_new_physician( p_npi_id IN VARCHAR2
                                 , p_physician_id OUT THOT.PHYSICIANS.id%TYPE
                                 , P_PHYS_ROW   NUMBER
                                 , p_request_id IN NUMBER --CQ27651
                                 )IS

      CURSOR cur_physician IS
         SELECT  *
           FROM THOT.MDM_PHYSICIAN_RESPONSE
          WHERE (npi_id=p_npi_id or dea_id = p_npi_id)
          AND   PHYS_ROW = P_PHYS_ROW
          AND ID_REQUEST=p_request_id --CQ27651
          order by creation_date desc;

      CURSOR C_MDM_PHYS_ADDR(P_REQ_ID NUMBER
                            ,p_dea_id varchar2 --++ added by CQ27587 Carlos Rasso
                             )  IS
   /*   select *
      FROM THOT.MDM_PHYSICIAN_ADDR_RESP
      WHERE ID_REQUEST     = P_REQ_ID
        AND mdm_addr_id    = P_DEA_ID; --++ added by CQ27587 Carlos Rasso condition added*/
        
      SELECT   distinct  name_type
                        , attn
                        , NAME
                        ,addr1
                        ,addr2
                        ,city
                        ,state
                        ,zip
                        ,comments
                       ,country
                       ,status
                       ,county
                       ,company_name
                       ,contact_first_name
                       ,contact_last_name
                       ,phy_clinic_flag
                       ,marketing_id
                       ,TRUNC(ADDR_EFFECTIVE_DATE) AS ADDR_EFFECTIVE_DATE
                       ,address_type
        FROM THOT.MDM_PHYSICIAN_ADDR_RESP 
       WHERE ID_REQUEST  = P_REQ_ID
         AND p_dea_id    = DECODE(DEA_OR_NPI,'NPI',p_dea_id,DEA_OR_NPI_VAL)
    ORDER BY  address_type ; -- jc


      cursor c_mdm_phys_phones(p_req_id number)  is
      select *
      from thot.mdm_physician_phones_resp
      where id_request = p_req_id;

      cursor c_mdm_phys_lic(p_req_id number)  is
      select *
      from thot.mdm_physician_attr_resp
      where id_request = p_req_id;

      cursor c_mdm_phys_dea(p_req_id number, p_dea_id varchar2) is
      select *
      FROM THOT.MDM_PHYSICIAN_DEA_RESP
      WHERE ID_REQUEST = P_REQ_ID
      AND PHYS_ROW = P_PHYS_ROW
      AND DEA_NUMBER = P_DEA_ID;
           

      MDM_PHYSICIAN_RESPONSE   THOT.MDM_PHYSICIAN_RESPONSE%ROWTYPE;
      r_mdm_phys_addr          thot.addresses%rowtype;
      r_mdm_phys_phone         thot.phones%rowtype;
      r_mdm_phys_lic           thot.attributes%rowtype;
      r_mdm_phys_dea           thot.physician_dea%rowtype;
      x_rowid                  ROWID;
      x_errors                 VARCHAR2(100);
      x_retcode                NUMBER := 0;
      v_code                   THOT.SPECIALTIES.CODE%TYPE := null;
      v_exist                  NUMBER:=0;
      v_next_id                NUMBER;
      v_name                  varchar2(100);
      V_COUNT                 NUMBER;
      V_STATE                 VARCHAR2(100):= NULL ; -- JC
      V_COUNT_SEQ             NUMBER := 0; -- JC


   BEGIN


      save_msg(1
               ,LABEL_INFO
               ,'Begin New physician insert ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.insert_new_physician'
               ,p_npi_id       --npi_id
               ,NULL --physician_id
               ,NULL --dea_id
               ,NULL --AIMI_HDR_ID
               ,g_process_name);
      OPEN cur_physician;
      FETCH cur_physician INTO MDM_PHYSICIAN_RESPONSE;
      CLOSE cur_physician;
         /*LOOP
            FETCH cur_physician INTO MDM_PHYSICIAN_RESPONSE;
            EXIT WHEN cur_physician%NOTFOUND;*/

            v_exist := exist_physician_dea(MDM_PHYSICIAN_RESPONSE.dea_id); --CQ27163
            v_name := MDM_PHYSICIAN_RESPONSE.LAST||', '||MDM_PHYSICIAN_RESPONSE.FIRST;

            FOR DEA IN C_MDM_PHYS_DEA(MDM_PHYSICIAN_RESPONSE.ID_REQUEST, MDM_PHYSICIAN_RESPONSE.DEA_ID) loop
              IF(v_exist=0) and MDM_PHYSICIAN_RESPONSE.dea_id is not null THEN
                  r_mdm_phys_dea.dea_number := dea.dea_number;
                  r_mdm_phys_dea.expired_flag := dea.expired_flag;
                  R_MDM_PHYS_DEA.BUSINESS_ACTIVITY_CODE := DEA.BUSINESS_ACTIVITY_CODE;
                  R_MDM_PHYS_DEA.ADDRESS1 := DEA.ADDRESS1; -- Added by CQ27616
                  r_MDM_phys_dea.address2 := dea.address2; -- Added by CQ27616
                  r_mdm_phys_dea.schedule1 := dea.schedule1;
                  r_mdm_phys_dea.schedule2 := dea.schedule2;
                  r_mdm_phys_dea.schedule2n := dea.schedule2n;
                  r_mdm_phys_dea.schedule3 := dea.schedule3;
                  r_mdm_phys_dea.schedule3n := dea.schedule3n;
                  r_mdm_phys_dea.schedule4 := dea.schedule4;
                  R_MDM_PHYS_DEA.SCHEDULE5 := DEA.SCHEDULE5;
                  R_MDM_PHYS_DEA.EXPIRATION_DATE := DEA.EXPIRATION_DATE;
                  r_mdm_phys_dea.city := dea.city;--nvl(dea.city, mdm_physician_response.city); -- modified by Carlos Rasso CQ27528 dea.city for npi.city
                  r_mdm_phys_dea.state := dea.state;
                  r_mdm_phys_dea.zip_code := dea.zip_code;
                  r_mdm_phys_dea.name := dea.name;
                  R_MDM_PHYS_DEA.SCHEDULEL1 := DEA.SCHEDULEL1;


                  INSERT INTO THOT.PHYSICIAN_DEA VALUES R_MDM_PHYS_DEA;
               END IF;
            end loop;

            --v_exist := exist_physician(MDM_PHYSICIAN_RESPONSE.npi_id); --CQ27163

--            IF(v_exist=0)THEN
               --v_code := get_specialty_code(MDM_PHYSICIAN_RESPONSE.specialty_code,MDM_PHYSICIAN_RESPONSE.description);


               SELECT THOT.PHYSICIANS_SEQ.nextval
                 INTO v_next_id
                 FROM dual;
               THOT.PHYSICIANS_PKG.insert_PRC( x_rowid
                                          , v_next_id
                                          , MDM_PHYSICIAN_RESPONSE.last
                                          , MDM_PHYSICIAN_RESPONSE.first
                                          , MDM_PHYSICIAN_RESPONSE.MI
                                          , MDM_PHYSICIAN_RESPONSE.MON_OF_BIRTH
                                          , MDM_PHYSICIAN_RESPONSE.DAY_OF_BIRTH
                                          , MDM_PHYSICIAN_RESPONSE.YR_OF_BIRTH
                                          , MDM_PHYSICIAN_RESPONSE.SPECIALTY_CODE
                                          --, MDM_PHYSICIAN_RESPONSE.license_id
                                          , null --CQ27136 Display purposes only
                                          , MDM_PHYSICIAN_RESPONSE.dea_id
                                          , MDM_PHYSICIAN_RESPONSE.UPIN
                                          , MDM_PHYSICIAN_RESPONSE.addr1
                                          , MDM_PHYSICIAN_RESPONSE.ADDR2
                                          , MDM_PHYSICIAN_RESPONSE.CITY
                                          , MDM_PHYSICIAN_RESPONSE.state
                                          , MDM_PHYSICIAN_RESPONSE.zip1
                                          , MDM_PHYSICIAN_RESPONSE.ZIP2
                                          , MDM_PHYSICIAN_RESPONSE.COUNTRY
                                          , MDM_PHYSICIAN_RESPONSE.COMMENTS
                                          , MDM_PHYSICIAN_RESPONSE.ADDR_LABEL_PRINTOK
                                          , MDM_PHYSICIAN_RESPONSE.TAX_ID
                                          , MDM_PHYSICIAN_RESPONSE.MEDICAID_ID
                                          , MDM_PHYSICIAN_RESPONSE.SOCSEC
                                          , MDM_PHYSICIAN_RESPONSE.EMPLOYMENT_START_DATE
                                          , MDM_PHYSICIAN_RESPONSE.EMPLOYMENT_STOP_DATE
                                          --, MDM_PHYSICIAN_RESPONSE.XUSER --CQ27136
                                          ,user--CQ27136
                                          --, NULL --CQ27119
                                          , MDM_PHYSICIAN_RESPONSE.INVOICER
                                          , MDM_PHYSICIAN_RESPONSE.MARKETING_ID
                                          , MDM_PHYSICIAN_RESPONSE.PRICING_ID
                                          , MDM_PHYSICIAN_RESPONSE.STATUS
                                          , MDM_PHYSICIAN_RESPONSE.phy_credential
                                          , MDM_PHYSICIAN_RESPONSE.INACTIVE_DATE
                                          , MDM_PHYSICIAN_RESPONSE.INACTIVATED_BY
                                          , MDM_PHYSICIAN_RESPONSE.STATUS_DEA_ID
                                          , MDM_PHYSICIAN_RESPONSE.npi_id
                                          , x_errors
                                          , x_retcode
                                          );

               --Insert physician State Licenses
               FOR L IN C_MDM_PHYS_LIC(MDM_PHYSICIAN_RESPONSE.ID_REQUEST) loop
                  r_mdm_phys_lic.NAME_TYPE := l.name_type;
                  r_mdm_phys_lic.NAME_ID := v_next_id;
                  r_mdm_phys_lic.ATTRIBUTE_TYPE := l.attribute_type;
                  r_mdm_phys_lic.ATTRIBUTE := l.attribute;
                  r_mdm_phys_lic.status := l.status;
                  r_mdm_phys_lic.state := l.state;
                  r_mdm_phys_lic.EXPIRATION_DATE := l.EXPIRATION_DATE;


                  thot.attributes_pkg.insert_prc(x_rowid,
                                             r_mdm_phys_lic,
                                             x_errors ,
                                             x_retcode);

                  if x_retcode <> 0 then

                     save_msg(1
                              ,LABEL_ERROR
                              ,'Unexpected error while inserting physician licenses information '||x_errors|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.insert_new_physician'
                              ,p_npi_id --npi_id
                              ,v_next_id --physician_id
                              ,MDM_PHYSICIAN_RESPONSE.dea_id --dea_id
                              ,NULL --AIMI_HDR_ID
                              ,g_process_name);
                     RAISE e_inserting_physician;
                  end if;
               end loop;


               SELECT COUNT(npi_id)
                 INTO v_count
                 FROM THOT.NPI_TABLE
                WHERE npi_id = MDM_PHYSICIAN_RESPONSE.npi_id;


               IF (v_count = 0 AND LENGTH(v_name)<=40) and MDM_PHYSICIAN_RESPONSE.npi_id is not null THEN
                  INSERT INTO THOT.NPI_TABLE (npi_id
                                          , name
                                          ,expire_date
                                          ,expired_flag
                                          , address
                                          , city
                                          , state
                                          , zip_code
                                          , creation_date
                                          , created_by
                                          , update_date
                                          , updated_by
                                          , entity_type_code )
                   VALUES (MDM_PHYSICIAN_RESPONSE.npi_id
                           , v_name
                           , NULL
                           ,'N'
                           , MDM_PHYSICIAN_RESPONSE.addr1
                           , MDM_PHYSICIAN_RESPONSE.city
                           , MDM_PHYSICIAN_RESPONSE.state
                           , MDM_PHYSICIAN_RESPONSE.zip1
                           , SYSDATE
                           , USER
                           , NULL
                           , NULL
                           , NULL );

               END IF;
               --JC START
       
                 BEGIN 
                   SELECT STATE
                     INTO v_state
                     FROM THOT.MDM_PHYSICIAN_ADDR_RESp 
                    WHERE ID_REQUEST = p_request_id
                      AND DEA_OR_NPI = 'DEA'
                      AND DEA_OR_NPI_VAL = MDM_PHYSICIAN_RESPONSE.dea_id
                      AND addr_seq = (SELECT min(addr_seq)
                                        FROM THOT.MDM_PHYSICIAN_ADDR_RESP P 
                                       WHERE P.ID_REQUEST =  p_request_id
                                         AND P.DEA_OR_NPI = 'DEA'
                                         AND P.DEA_OR_NPI_VAL = MDM_PHYSICIAN_RESPONSE.dea_id );
                 EXCEPTION 
                    WHEN NO_DATA_FOUND THEN 
                        v_state := NULL ; --> STATE WILL BE NULL THEREFORE NO ADDRESS WILL BE INSERTED 
                        
                 END ; 
               --JC END
               
               --Insert physician address
               FOR A IN  C_MDM_PHYS_ADDR(MDM_PHYSICIAN_RESPONSE.ID_REQUEST
                                        ,MDM_PHYSICIAN_RESPONSE.mdm_addr_id --++ added by CQ27587 Carlos Rasso
                                        ) loop
                                        
                                 
                 IF  v_state =  a.state   THEN     --JC 
                     V_COUNT_SEQ  := V_COUNT_SEQ + 1 ;                 
                     r_mdm_phys_addr.NAME_TYPE := a.name_type;
                     r_mdm_phys_addr.NAME_ID := v_next_id;
                     r_mdm_phys_addr.ADDR_SEQ := V_COUNT_SEQ ; --a.addr_seq;   --logic TBD
                     r_mdm_phys_addr.ATTN := a.attn;
                     r_mdm_phys_addr.NAME := A.NAME;
                     r_mdm_phys_addr.ADDR1 := a.addr1;
                     r_mdm_phys_addr.ADDR2 := a.addr2;
                     r_mdm_phys_addr.CITY := a.city;
                     r_mdm_phys_addr.STATE := a.state;
                     r_mdm_phys_addr.ZIP := a.zip;
                     r_mdm_phys_addr.COMMENTS := a.comments;
                     if V_COUNT_SEQ = 1 AND a.address_type != 'OFFICE' THEN 
                        r_mdm_phys_addr.ADDRESS_TYPE := 'OFFICE';
                     else
                        r_mdm_phys_addr.ADDRESS_TYPE := a.address_type;
                     end if;
                     
                     r_mdm_phys_addr.COUNTRY := a.country;
                     r_mdm_phys_addr.STATUS := a.status;
                     r_mdm_phys_addr.COUNTY := a.county;
                     ---r_mdm_phys_addr.PARTNERSHIP_SECURITY_LABEL := null;
                     r_mdm_phys_addr.COMPANY_NAME := a.company_name;
                     r_mdm_phys_addr.CONTACT_FIRST_NAME := a.contact_first_name;
                     r_mdm_phys_addr.CONTACT_LAST_NAME := a.contact_last_name;
                     r_mdm_phys_addr.PHY_CLINIC_FLAG := a.phy_clinic_flag;
                     r_mdm_phys_addr.MARKETING_ID := a.marketing_id;
                     R_MDM_PHYS_ADDR.ADDR_EFFECTIVE_DATE := A.ADDR_EFFECTIVE_DATE;

                     thot.addresses_pkg.insert_PRC(x_rowid
                                                 , r_mdm_phys_addr
                                                 , x_errors
                                                 , x_retcode );
                      if x_retcode <> 0 then
                         save_msg(1
                                  ,LABEL_ERROR
                                  ,'Unexpected error while inserting address information '||x_errors|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.insert_new_physician'
                                  ,p_npi_id --npi_id
                                  ,v_next_id --physician_id
                                  ,MDM_PHYSICIAN_RESPONSE.dea_id --dea_id
                                  ,NULL --AIMI_HDR_ID
                                  ,g_process_name);
                      end if;
                  END IF;   --JC    
               end loop;
               --Insert phones in main table
               for p in c_mdm_phys_phones(MDM_PHYSICIAN_RESPONSE.id_request) loop
                r_mdm_phys_phone.NAME_TYPE := p.NAME_TYPE;
                r_mdm_phys_phone.NAME_ID := v_next_id;
                r_mdm_phys_phone.PHONE_SEQ := p.PHONE_SEQ;
                r_mdm_phys_phone.PHONE_TYPE := p.PHONE_TYPE;
                r_mdm_phys_phone.PHONEA := p.PHONEA;
                r_mdm_phys_phone.PHONEB := p.PHONEB;
                r_mdm_phys_phone.PHONEC := p.PHONEC;
                r_mdm_phys_phone.ADDR_SEQ := p.ADDR_SEQ;

                  thot.phones_pkg.insert_PRC(x_rowid
                                          , r_mdm_phys_phone
                                          , x_errors
                                          , x_retcode );

                  if x_retcode <> 0 then
                        save_msg(1
                                 ,LABEL_ERROR
                                 ,'Unexpected error while inserting phones information '||x_errors|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.insert_new_physician'
                                 ,p_npi_id --npi_id
                                 ,v_next_id --physician_id
                                 ,MDM_PHYSICIAN_RESPONSE.dea_id --dea_id
                                 ,NULL --AIMI_HDR_ID
                                 ,g_process_name);
                  END IF;
               end loop;

               --Disable the record received from the web service.
               UPDATE thot.MDM_PHYSICIAN_RESPONSE
                  SET row_used = 1
                WHERE npi_id = MDM_PHYSICIAN_RESPONSE.npi_id
                  AND row_used = 0;

               COMMIT;
               p_physician_id := v_next_id;
--            END IF;

            /*IF(x_retcode!=0)THEN
               RAISE e_inserting_physician;
            END IF;*/
         --END LOOP;
     COMMIT;
      save_msg(1
               ,LABEL_INFO
               ,'End New physician insert ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.insert_new_physician'
               ,p_npi_id --npi_id
               ,v_next_id --physician_id
               ,MDM_PHYSICIAN_RESPONSE.dea_id --dea_id
               ,NULL --AIMI_HDR_ID
               ,g_process_name);
   EXCEPTION
      /*WHEN e_inserting_physician THEN
            IF(cur_physician%ISOPEN)THEN
               CLOSE cur_physician;
            END IF;
             save_msg(1
               ,LABEL_ERROR
               ,'FATAL: Unexpected error while inserting main physician record'||x_errors|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.insert_new_physician'
               ,p_npi_id --npi_id
               ,null --physician_id
               ,null
               ,g_process_name);

            ROLLBACK;*/
       WHEN OTHERS THEN
       save_msg(1
               ,LABEL_ERROR
               ,'FATAL: Unexpected error while inserting physician records'||sqlerrm|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.insert_new_physician'
               ,p_npi_id --npi_id
               ,null --physician_id
               ,null --dea_id
					,NULL --AIMI_HDR_ID
               ,G_PROCESS_NAME);

            ROLLBACK;
   END insert_new_physician;



-- +---------------------------------------------------------------------------+
-- | Name: create_message   CQ27045                                            |
-- | Description: Create the XML message.                                      |
-- | Parameters                                                                |
-- |     p_npi                                                                 |
-- +---------------------------------------------------------------------------+
FUNCTION create_message(p_value THOT.MDM_PHYSICIAN_RESPONSE.npi_id%TYPE, p_qualifier VARCHAR2)
            RETURN VARCHAR IS
   v_message   VARCHAR2(4000);
   BEGIN
   DBMS_OUTPUT.PUT_LINE('enter');
      IF(p_qualifier = 'NPI' OR p_qualifier = 'DEA')THEN
         v_message := '<?xml version="1.0" encoding="UTF-8"?>';
         v_message := v_message || '<TCRMService xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="myTCRM.xsd">';
         v_message := v_message || '<RequestControl>';
            v_message := v_message || '<requestID>1000000</requestID>';
            v_message := v_message || '<DWLControl>';
                  v_message := v_message || '<requesterName>Prescriber Service</requesterName>';
                  v_message := v_message || '<requesterLanguage>100</requesterLanguage>';  --English
                  v_message := v_message || '<lineOfBusiness>1000013</lineOfBusiness>';
            v_message := v_message || '</DWLControl>';
         v_message := v_message || '</RequestControl>';
         v_message := v_message || '<TCRMInquiry>';
            v_message := v_message || '<InquiryType>getPrescriberDetails</InquiryType>';
            v_message := v_message || '<InquiryParam>';
               v_message := v_message || '<tcrmParam name="InquiryLevel">106</tcrmParam>';
               v_message := v_message || '<tcrmParam name="PrescriberId">' || p_value ||'</tcrmParam>';
               v_message := v_message || '<tcrmParam name="PrescriberQualifier">' || p_qualifier ||'</tcrmParam>';
               v_message := v_message ||'<tcrmParam name="StateCode">'||'</tcrmParam>';
               v_message := v_message || '<tcrmParam name="LastName"></tcrmParam>';
                v_message := v_message || '<tcrmParam name="ZipCode"></tcrmParam>';
            v_message := v_message || '</InquiryParam>';
         v_message := v_message || '</TCRMInquiry>';
         v_message := v_message || '</TCRMService>';
      ELSE
         RAISE e_invalid_qualifier;
      END IF;

      RETURN v_message;
   END create_message;


-- +---------------------------------------------------------------------------+
-- | Name: send_message    CQ27045                                             |
-- | Description: Send the message to the queue. This message will be process  |
-- |               for aimi                                                    |
-- | Parameters                                                                |
-- |     p_npi                                                                 |
-- +---------------------------------------------------------------------------+
FUNCTION send_message(p_value THOT.MDM_PHYSICIAN_RESPONSE.npi_id%TYPE, p_qualifier VARCHAR2)
                         return NUMBER IS
       v_errors                                                                        VARCHAR2(4000);
       v_retcode                                                                    NUMBER;
       v_id_request              RXH_CUSTOM.WEB_SERVICES_TRANS.ID%TYPE:=0;
       v_excep_id                RXH_CUSTOM.WEB_SERVICES_TRANS.ID%TYPE:=0;
       v_excep_status            RXH_CUSTOM.WEB_SERVICES_TRANS.STATUS%TYPE;
       c_max_retrys              NUMBER:=3;
       c_time_out                NUMBER:=1;
       v_number_retry            NUMBER:=0;
       v_connect_type            VARCHAR2(5):='HTTP'; --HTTP OR HTTPS

   BEGIN
      save_msg(1
               ,LABEL_INFO
               ,'Begin Send request ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.send_message'
               ,p_value --NPI_ID
               ,NULL --PHYSICIAN_ID
               ,NULL --DEA_ID
					,NULL --AIMI_HDR_ID
               ,g_process_name);
      SELECT attribute1
        INTO v_connect_type
        FROM THOT.FT_LOOKUPS
       WHERE TYPE        = 'PRT_CONNECTION_TYPE'
        AND CODE        = 'PHYSICIAN_ENRICHMENT'
         AND STATUS  = 'A';

                RXH_CUSTOM.RXC_AIMI_PROCESS_PKG.ENQUEUE_AIMI_MESSAGE
         ( p_process_name        => 'NEW_PHYSIC'
          ,p_message_text => create_message(p_value, p_qualifier)
          ,p_connect_type       => v_connect_type
          ,p_property_3      => p_value
          ,x_errors          => v_errors
          ,x_retcode         => v_retcode );
      COMMIT;

      SELECT attribute1
        INTO c_max_retrys --MAX RETRY NUMBER
        FROM THOT.FT_LOOKUPS
       WHERE TYPE        = 'PRT_PHYS_ENRICHMENT_RETRY'
         AND CODE        = 'PHYSICIAN_ENRICHMENT'
         AND STATUS      = 'A';

      SELECT attribute1
        INTO c_time_out
        FROM THOT.FT_LOOKUPS
       WHERE TYPE        = 'PRT_PHYS_ENRICHMENT_TIMEOUT'
         AND CODE        = 'PHYSICIAN_ENRICHMENT'
         AND STATUS  = 'A';

      WHILE v_id_request = 0 AND v_number_retry<c_max_retrys
      LOOP
            DBMS_LOCK.Sleep(c_time_out);
            BEGIN
            SELECT mdm.id_request
              INTO v_id_request
              FROM RXH_CUSTOM.WEB_SERVICES_TRANS ws, THOT.MDM_PHYSICIAN_RESPONSE mdm
             WHERE ws.ID = mdm.id_request
               AND (mdm.npi_id = p_value or mdm.dea_id = p_value)  --join by DEA or NPI value
               AND ws.tx_id=p_value
               AND ws.status= 'RESPONSE_RECEIVED'
               AND mdm.update_date IS NULL
               AND mdm.updated_by IS NULL
               AND mdm.creation_date >= sysdate - 3/(60*60*24);


            EXCEPTION
            WHEN OTHERS THEN
               v_number_retry := v_number_retry + 1;
            END;
      END LOOP;

      IF(v_id_request != 0)THEN
         UPDATE THOT.MDM_PHYSICIAN_RESPONSE
            SET update_date = SYSDATE
                ,updated_by = USER
          WHERE id_request = v_id_request;
          COMMIT;
      ELSE  --exception scenario
         select id
         into v_excep_id
         from RXH_CUSTOM.WEB_SERVICES_TRANS
         where  tx_id = p_value
         and creation_date = (select max(creation_date)
                              from RXH_CUSTOM.WEB_SERVICES_TRANS
                              where  tx_id = p_value
                              );

         v_excep_status := parse_and_validate_found(v_excep_id);

         update RXH_CUSTOM.WEB_SERVICES_TRANS
         set status = nvl(v_excep_status,status)
         where id = v_excep_id;

         commit;
         IF v_excep_status = 'PHYS_NOT_FOUND' THEN
            save_msg(1
                     ,LABEL_INFO
                     ,'Physician not found in MDM service: '||p_qualifier|| '('||p_value||') not found' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.send_message'
                     ,p_value --NPI_ID
                     ,NULL --PHYSICIAN_ID
                     ,NULL --DEA_ID
							,NULL --AIMI_HDR_ID
                     ,g_process_name);
            RETURN -1;
         END IF;
      END IF;

   save_msg(1
            ,LABEL_INFO
            ,'End Send request, response received v_id_request='||v_id_request || ' - THOT.PHYSICIANS_CLEAN_UP_PKG.send_message'
            ,p_value --NPI_ID
            ,NULL    --PHYSICIAN_ID
            ,NULL    --DEA_ID
				,NULL    --AIMI_HDR_ID
            ,g_process_name);
   RETURN v_id_request;
   EXCEPTION
      WHEN NO_DATA_FOUND THEN
         save_msg(1,
                  LABEL_ERROR
                  ,SQLERRM|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.send_message. No Response Found from service.'
                  ,p_value --NPI_ID
                  ,null --PHYSICIAN_ID
                  ,null --DEA_ID
						,NULL --AIMI_HDR_ID
                  ,g_process_name);
         RETURN 0;
       WHEN OTHERS THEN
         save_msg(1,
                  LABEL_ERROR
                  ,SQLERRM|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.send_message.'
                  ,p_value  --NPI_ID
                  ,null     --PHYSICIAN_ID
                  ,null     --DEA_ID
						,NULL     --AIMI_HDR_ID
                  ,g_process_name);
         RETURN 0;
   END send_message;


   -- +---------------------------------------------------------------------------+
-- | Name: exist_physician_dea                                                 |
-- | Description: Consult if the DEA ID exists on PHYSICIAN_DEA table.         |
-- | CQ27045                                                                   |
-- | Parameters:                                                               |
-- |     p_dea_number = DEA_ID                                                 |
-- | Return:                                                                   |
-- |     If exists 1                                                           |
-- |     If not exist 0                                                        |
-- +---------------------------------------------------------------------------+

   FUNCTION exist_physician_dea(p_dea_number THOT.PHYSICIAN_DEA.dea_number%TYPE)
                               RETURN NUMBER IS
      v_dea_id                 THOT.PHYSICIAN_DEA.dea_number%TYPE:=NULL;
   BEGIN

       SELECT dea_number
         INTO v_dea_id
         FROM THOT.PHYSICIAN_DEA
        WHERE dea_number = p_dea_number;
      RETURN 1;

   EXCEPTION
      WHEN NO_DATA_FOUND THEN
         RETURN 0;

   END exist_physician_dea;



-- +---------------------------------------------------------------------------+
-- | Name: exist_physician                                                     |
-- | Description: Consult if the NPI exists on PHYSICIANS table.               |
-- |  CQ27045                                                                  |
-- | Parameters:                                                               |
-- |     p_npi_id = npi_id                                                     |
-- | Return:                                                                   |
-- |     If exists 1                                                           |
-- |     If not exist 0                                                        |
-- +---------------------------------------------------------------------------+
   FUNCTION exist_physician(p_npi_id THOT.PHYSICIANS.npi_id%TYPE)
                              RETURN NUMBER IS
       v_physician_id         THOT.PHYSICIANS.id%TYPE:=NULL;
   BEGIN

      SELECT id
        INTO v_physician_id
        FROM THOT.PHYSICIANS
       WHERE npi_id=p_npi_id;
      RETURN 1;

   EXCEPTION
      WHEN NO_DATA_FOUND THEN
         RETURN 0;

   END exist_physician;



-- +---------------------------------------------------------------------------+
-- | Name: get_specialty_code  CQ27045                                         |
-- | Description: Retrieve the specialty code to add the physician to          |
-- |           PHYSICIANS table. Check if the code exists on SPECIALTIES table.|
-- | Parameters (Example):                                                     |
-- |     p_code = "OP"                                                         |
-- |     p_description ="OPHTHALMOLOGY"                                        |
-- | Return:                                                                   |
-- |     code: Specialty code                                                  |
-- |                                                                           |
-- +---------------------------------------------------------------------------+
   FUNCTION get_specialty_code(p_code THOT.SPECIALTIES.CODE%TYPE
                              ,p_description THOT.SPECIALTIES.DESCRIPTION%TYPE)
                              return THOT.SPECIALTIES.CODE%TYPE IS
      v_code               THOT.SPECIALTIES.CODE%TYPE := null;

   BEGIN
      SELECT code
        INTO v_code
        FROM THOT.SPECIALTIES
       WHERE code = p_code
          OR description = p_description;

     RETURN v_code;
   EXCEPTION
      WHEN NO_DATA_FOUND THEN
         RETURN insert_specialty(p_code,p_description);
   END get_specialty_code;



-- +---------------------------------------------------------------------------+
-- | Name: insert_specialty  CQ27045                                           |
-- | Description: Consult if the CODE exists on SPEICALTIES table. If not      |
-- |         insert the new specialty.                                         |
-- | Parameters:                                                               |
-- |     p_dea_number = DEA_ID                                                 |
-- | Return:                                                                   |
-- |     Specialty code                                                        |
-- |                                                                           |
-- +---------------------------------------------------------------------------+
      FUNCTION insert_specialty(p_code THOT.SPECIALTIES.code%TYPE
                              , p_description THOT.SPECIALTIES.description%TYPE)
                              RETURN VARCHAR2 IS
      BEGIN

            IF(p_code IS NOT NULL AND p_description IS NOT NULL)THEN

               INSERT
                 INTO thot.specialties
                     (code
                     ,description
                     ,created_by
                     ,creation_date
                     ,updated_by
                     ,update_date)
               VALUES(p_code
                     ,p_description
                     ,USER
                     ,SYSDATE
                     ,null
                     ,null);
               COMMIT;
               RETURN p_code;
            ELSE
               RETURN NULL;
            END IF;
      EXCEPTION
      WHEN OTHERS THEN
         save_msg(1
                  ,LABEL_ERROR
                  ,SQLERRM|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.insert_specialty'
                  ,NULL --NPI_ID
                  ,NULL --PHYSICIAN_ID
                  ,NULL --DEA_ID
						,NULL --AIMI_HDR_ID
                  ,g_process_name);

      END;

-- +---------------------------------------------------------------------------+
-- | Name: insert_specialty  CQ27045                                           |
-- | Description: Consult if the CODE exists on SPEICALTIES table. If not      |
-- |         insert the new specialty.                                         |
-- | CQ27045
-- | Parameters:                                                               |
-- |     p_dea_number = DEA_ID                                                 |
-- | Return:                                                                   |
-- |     Specialty code                                                        |
-- |                                                                           |
-- +---------------------------------------------------------------------------+

   PROCEDURE update_physician(p_npi_id             IN THOT.PHYSICIANS.npi_id%TYPE
                              ,p_physician_id      IN THOT.PHYSICIANS.id%TYPE
                              ,P_ROW_ID            IN NUMBER
                              ,p_request_id        IN NUMBER --CQ27651
                              ,X_RETCODE           OUT NUMBER
                              ,x_error_code        OUT VARCHAR2
                              )IS

      MDM_PHYSICIAN_RESPONSE        THOT.MDM_PHYSICIAN_RESPONSE%ROWTYPE;
      p_physician              THOT.PHYSICIANS%ROWTYPE;
      cursor c_phys_request is
      SELECT *
      --INTO MDM_PHYSICIAN_RESPONSE
      FROM THOT.MDM_PHYSICIAN_RESPONSE
      WHERE (npi_id = p_npi_id or dea_id = p_npi_id) --CQ26096
      and phys_row = p_row_id
      AND ROW_USED = 0
      AND ID_REQUEST = p_request_id; --CQ27651

      v_update_dea   varchar2(1);
      v_cnt_npi      number := 0;
      v_name         varchar2(300);
   BEGIN
      save_msg(1
               ,LABEL_INFO
               ,'Begin update physician '|| 'THOT.PHYSICIANS_CLEAN_UP_PKG.update_physician'
               ,p_npi_id --NPI_ID
               ,p_physician_id --PHYSICIAN_ID
               ,NULL --DEA_ID
               ,NULL --AIMI_HDR_ID
               ,g_process_name);
            open c_phys_request;
            fetch c_phys_request into MDM_PHYSICIAN_RESPONSE;
            close c_phys_request;

          --Validates if DEA is present to update it
          if MDM_PHYSICIAN_RESPONSE.dea_id is not null then
             update_phys_dea(p_physician_id
                           ,mdm_physician_response
                           ,v_update_dea);

             if v_update_dea = 'N' then
               MDM_PHYSICIAN_RESPONSE.dea_id := null;
               MDM_PHYSICIAN_RESPONSE.status_dea_id := null;
             end if;
          end if;
          --Validates if NPI is present to update it.
           SELECT COUNT(npi_id)
           INTO v_cnt_npi
           FROM THOT.NPI_TABLE
           WHERE npi_id = MDM_PHYSICIAN_RESPONSE.npi_id;

           v_name := MDM_PHYSICIAN_RESPONSE.LAST||', '||MDM_PHYSICIAN_RESPONSE.FIRST;

          IF (v_cnt_npi = 0 AND LENGTH(v_name)<=40) and MDM_PHYSICIAN_RESPONSE.npi_id is not null THEN
                  INSERT INTO THOT.NPI_TABLE (npi_id
                                          , name
                                          ,expire_date
                                          ,expired_flag
                                          , address
                                          , city
                                          , state
                                          , zip_code
                                          , creation_date
                                          , created_by
                                          , update_date
                                          , updated_by
                                          , entity_type_code )
                   VALUES (MDM_PHYSICIAN_RESPONSE.npi_id
                           , v_name
                           , NULL
                           ,'N'
                           , MDM_PHYSICIAN_RESPONSE.addr1
                           , MDM_PHYSICIAN_RESPONSE.city
                           , MDM_PHYSICIAN_RESPONSE.state
                           , MDM_PHYSICIAN_RESPONSE.zip1
                           , SYSDATE
                           , USER
                           , NULL
                           , NULL
                           , NULL );

          --ELSE
            --MDM_PHYSICIAN_RESPONSE.npi_id := null; CQ27119 Fix this scenario
          END IF;

          UPDATE THOT.PHYSICIANS SET
             /*SET license_id = COALESCE(license_id, MDM_PHYSICIAN_RESPONSE.license_id)
                ,specialty_code = COALESCE(specialty_code, MDM_PHYSICIAN_RESPONSE.specialty_code)
                ,first = COALESCE(first, MDM_PHYSICIAN_RESPONSE.first)
                ,update_date =SYSDATE
                ,updated_by = USER*/
               last =  COALESCE(last, MDM_PHYSICIAN_RESPONSE.last),
               first =  COALESCE(first, MDM_PHYSICIAN_RESPONSE.first),
               mi =  COALESCE(mi, MDM_PHYSICIAN_RESPONSE.mi),
               mon_of_birth =  COALESCE(mon_of_birth, MDM_PHYSICIAN_RESPONSE.mon_of_birth),
               day_of_birth =  COALESCE(day_of_birth, MDM_PHYSICIAN_RESPONSE.day_of_birth),
               yr_of_birth =  COALESCE(yr_of_birth, MDM_PHYSICIAN_RESPONSE.yr_of_birth),
               specialty_code =  COALESCE(specialty_code, MDM_PHYSICIAN_RESPONSE.specialty_code),
               license_id =  COALESCE(license_id, MDM_PHYSICIAN_RESPONSE.license_id),
               dea_id =  COALESCE(dea_id, MDM_PHYSICIAN_RESPONSE.dea_id),
               upin =  COALESCE(upin, MDM_PHYSICIAN_RESPONSE.upin),
               addr1 =  COALESCE(addr1, MDM_PHYSICIAN_RESPONSE.addr1),
               addr2 =  COALESCE(addr2, MDM_PHYSICIAN_RESPONSE.addr2),
               city =  COALESCE(city, MDM_PHYSICIAN_RESPONSE.city),
               state =  COALESCE(state, MDM_PHYSICIAN_RESPONSE.state),
               zip1 =  COALESCE(zip1, MDM_PHYSICIAN_RESPONSE.zip1),
               zip2 =  COALESCE(zip2, MDM_PHYSICIAN_RESPONSE.zip2),
               country =  COALESCE(country, MDM_PHYSICIAN_RESPONSE.country),
               comments =  COALESCE(comments, MDM_PHYSICIAN_RESPONSE.comments),
               addr_label_printok =  COALESCE(addr_label_printok, MDM_PHYSICIAN_RESPONSE.addr_label_printok),
               tax_id =  COALESCE(tax_id, MDM_PHYSICIAN_RESPONSE.tax_id),
               medicaid_id =  COALESCE(medicaid_id, MDM_PHYSICIAN_RESPONSE.medicaid_id),
               socsec =  COALESCE(socsec, MDM_PHYSICIAN_RESPONSE.socsec),
               employment_start_date =  COALESCE(employment_start_date, MDM_PHYSICIAN_RESPONSE.employment_start_date),
               employment_stop_date =  COALESCE(employment_stop_date, MDM_PHYSICIAN_RESPONSE.employment_stop_date),
               --xuser =  user, --CQ27119
               xuser =  COALESCE(xuser, user),
               invoicer =  COALESCE(invoicer, MDM_PHYSICIAN_RESPONSE.invoicer),
               marketing_id =  COALESCE(marketing_id, MDM_PHYSICIAN_RESPONSE.marketing_id),
               pricing_id =  COALESCE(pricing_id, MDM_PHYSICIAN_RESPONSE.pricing_id),
               status =  COALESCE(status, MDM_PHYSICIAN_RESPONSE.status),
               phy_credential =  COALESCE(phy_credential, MDM_PHYSICIAN_RESPONSE.phy_credential),
               inactive_date =  COALESCE(inactive_date, MDM_PHYSICIAN_RESPONSE.inactive_date),
               inactivated_by =  COALESCE(inactivated_by, MDM_PHYSICIAN_RESPONSE.inactivated_by),
               update_date =  sysdate,
               updated_by =  user,
               npi_id =  COALESCE(npi_id, MDM_PHYSICIAN_RESPONSE.npi_id),
               status_dea_id =  COALESCE(status_dea_id, MDM_PHYSICIAN_RESPONSE.status_dea_id)
           WHERE id = p_physician_id
             --AND npi_id = p_npi_id
             ;

         --Update Addresses
          update_phys_addresses( p_physician_id  => p_physician_id
                                ,P_REQUEST_ID    => MDM_PHYSICIAN_RESPONSE.ID_REQUEST
                                ,p_dea_id        => mdm_physician_response.dea_id --++ added by CQ27587 Carlos Rasso
                                );
          update_phys_phones( p_physician_id  => p_physician_id
                            ,p_request_id   => MDM_PHYSICIAN_RESPONSE.id_request);

         --Update phones

         --Disable the record received from the web service.
          UPDATE THOT.MDM_PHYSICIAN_RESPONSE
             SET row_used = 1
           WHERE npi_id = MDM_PHYSICIAN_RESPONSE.npi_id
             AND row_used = 0;


          COMMIT;
          IF(SQL%ROWCOUNT>0)THEN
            x_retcode := 1;
          ELSE
            x_retcode := 0;
          END IF;
          save_msg(1
               ,LABEL_INFO
               ,'End update physician '|| 'THOT.PHYSICIANS_CLEAN_UP_PKG.update_physician'
               ,p_npi_id --NPI_ID
               ,p_physician_id --PHYSICIAN_ID
               ,NULL --DEA_ID
               ,NULL --AIMI_HDR_ID
               ,g_process_name);
   EXCEPTION
      WHEN OTHERS THEN
      save_msg(1
               ,LABEL_ERROR
               ,'FATAL: Physician cannot be updated'||sqlerrm|| 'THOT.PHYSICIANS_CLEAN_UP_PKG.update_physician'
               ,p_npi_id --NPI_ID
               ,p_physician_id --PHYSICIAN_ID
               ,NULL --DEA_ID
               ,NULL --AIMI_HDR_ID
               ,g_process_name);

         x_retcode :=SQLCODE;
         x_error_code := SQLERRM;
   END update_physician;

-- US6854
-- +============================================================================================================+
-- |   Name: parse_phys_mdm_response                                                                             |
-- |   Description: This Procedure will parse the incoming XML and  insert the data into staging tables for MDM  |
-- |   Parameters: p_response_id : Id that identifies the WS response. AIMI_HDRiD Value                          |
-- |               p_xml     : Incoming XML repsonse                                                             |
-- |   Author: Diego Resendi CQ27045                                                                             |
-- +============================================================================================================+
  procedure parse_phys_mdm_response(p_response_id number, p_xml   clob)
   is --pragma autonomous_transaction;
      r_phys_row  thot.mdm_physician_response%rowtype := null; --thot.physicians%rowtype:=null;
      r_attributes_lic  thot.mdm_physician_attr_resp%rowtype:=null;
      r_addresses thot.mdm_physician_addr_resp%rowtype := null;
      r_phones          thot.mdm_physician_phones_resp%rowtype := null;
      r_phys_dea        thot.mdm_physician_dea_resp%rowtype := null;
      v_phone_str_aux   varchar2(30);
      v_phone_seq_oth   number := 3;
      v_occur           number;
      v_phones_types_ins   varchar2(500):=null;
      v_dea_last_occur  number:=0;
      v_mdm_addr_type   VARCHAR2(10):='DEA';
      v_addr_init_seq   NUMBER := 1;  -- modified by CQ27497 JGonzalez Mar-11-2015
      -- CQ27497 JGonzalez Mar-11-2015 [
      v_found           NUMBER := 0;
      v_addr_seq1_found NUMBER := 0;
      V_COUNT_ADDR      NUMBER := 0;
      -- ] CQ27497 JGonzalez Mar-11-2015
      v_null           varchar2(1) := 'N';

      -- Added by Carlos Rasso CQ27528 Begin [

      aux_addr1         VARCHAR2(250);
      aux_addr2         VARCHAR2(250);
      aux_addrt         VARCHAR2(500);
      END_OF_ADDR       NUMBER;

      -- ] End CQ27528
      NU_NPI_ADDRS NUMBER:=0;       -- added by JOSE GONZALEZ CQ27616 APR-02-2015
      nu_addr_id   NUMBER:=0;       -- Added by CRasso/CRojas CQ27651

   begin
      --commence mapping
      --Physician record
      save_msg(1
            ,LABEL_INFO
            ,'Begin response parsing.' || ' THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
            ,NULL --NPI_ID
            ,NULL --PHYSICIAN_ID
            ,NULL --DEA_ID
            ,p_response_id --AIMI_HDR_ID
            ,g_process_name);
      v_occur := get_occurrences_by_tag_val(p_xml,'IdentificationValue','DEA');
      if v_occur = 0 then
         save_msg(1
                  ,LABEL_WARNING
                  ,'DEA is not present'|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
                  ,NULL --NPI_ID
                  ,NULL --PHYSICIAN_ID
                  ,NULL --DEA_ID
                  ,p_response_id --AIMI_HDR_ID
                  ,g_process_name);
         v_occur := get_occurrences_by_tag_val(p_xml,'IdentificationValue','NPI');
         v_mdm_addr_type:= 'NPI';
      end if;
      FOR P IN 0..V_OCCUR-1 LOOP
         r_phys_row.id_request := p_response_id;
         r_phys_row.phys_row := p+1;
         r_phys_row.npi_id := get_tag_value(p_xml,'IdentificationNumber',get_tag_pos_by_val(p_xml,'IdentificationValue','NPI'));
         if r_phys_row.npi_id is null then
         save_msg(1
                  ,LABEL_WARNING
                  ,'NPI is not present'|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
                  ,NULL
                  ,NULL
                  ,NULL
                  ,p_response_id
                  ,g_process_name);
         END IF;
       --  r_phys_row.dea_id := get_tag_value(p_xml,'IdentificationNumber',get_tag_pos_by_val(p_xml,'IdentificationValue','DEA',p));
         --r_phys_row.dea_id := get_tag_value(p_xml,'IdentificationNumber',get_tag_pos_by_val(p_xml,'IdentificationValue','DEA',v_dea_last_occur));
         r_phys_row.dea_id := get_tag_value(p_xml,'IdentificationNumber',get_occur_index_by_tag_val(p_xml,'IdentificationValue','DEA',p+1));

         if v_mdm_addr_type = 'NPI' then
            R_PHYS_ROW.MDM_ADDR_ID := R_PHYS_ROW.NPI_ID;
         ELSE
            R_PHYS_ROW.MDM_ADDR_ID := R_PHYS_ROW.DEA_ID;

         end if;
         --v_dea_last_occur:=get_tag_pos_by_val(p_xml,'IdentificationValue','DEA',p);
         if r_phys_row.dea_id is not null then
            r_phys_row.status_dea_id := 'A';
         end if;
         --r_phys_row.first := upper(get_tag_value(p_xml,'GivenNameOne',get_tag_pos_by_val(p_xml,'SourceIdentifierValue','CMS'))||' '||upper(get_tag_value(p_xml,'GivenNameTwo',get_tag_pos_by_val(p_xml,'SourceIdentifierValue','CMS'))));
         r_phys_row.first := upper(get_tag_value(p_xml,'GivenNameOne',get_tag_pos_by_val(p_xml,'SourceIdentifierValue','CMS')));
         r_phys_row.mi := substr(upper(get_tag_value(p_xml,'GivenNameTwo',get_tag_pos_by_val(p_xml,'SourceIdentifierValue','CMS'))),1,1);
         r_phys_row.last := upper(get_tag_value(p_xml,'LastName',get_tag_pos_by_val(p_xml,'SourceIdentifierValue','CMS')));
         r_phys_row.license_id := get_tag_value(p_xml,'IdentificationNumber',get_tag_pos_by_val(p_xml,'IdentificationValue','STLC')); --CQ27136

         --For physician main address, go look for XSourceValue = CMS
         r_phys_row.city := upper(get_tag_value(p_xml,'City',get_tag_pos_by_val(p_xml,'XSourceValue','CMS')));
         r_phys_row.state := upper(get_tag_value(p_xml,'ProvinceStateValue',get_tag_pos_by_val(p_xml,'XSourceValue','CMS')));
         r_phys_row.zip1 := substr(get_tag_value(p_xml,'ZipPostalCode',get_tag_pos_by_val(p_xml,'XSourceValue','CMS')),1,5);
         R_PHYS_ROW.ZIP2 := SUBSTR(GET_TAG_VALUE(P_XML,'ZipPostalCode',GET_TAG_POS_BY_VAL(P_XML,'XSourceValue','CMS')),6,4);

         if r_phys_row.last is null then  --if no NPI information is present, use DEA's
            r_phys_row.first := upper(get_tag_value(p_xml,'GivenNameOne',get_tag_pos_by_val(p_xml,'SourceIdentifierValue','NTIS'))||' '||upper(get_tag_value(p_xml,'GivenNameTwo',get_tag_pos_by_val(p_xml,'SourceIdentifierValue','NTIS'))));
            r_phys_row.last := upper(get_tag_value(p_xml,'LastName',get_tag_pos_by_val(p_xml,'SourceIdentifierValue','NTIS')));
            r_phys_row.city := upper(get_tag_value(p_xml,'City',get_tag_pos_by_val(p_xml,'XSourceValue','NTIS')));
            r_phys_row.state := upper(get_tag_value(p_xml,'ProvinceStateValue',get_tag_pos_by_val(p_xml,'XSourceValue','NTIS')));
            r_phys_row.zip1 := substr(get_tag_value(p_xml,'ZipPostalCode',get_tag_pos_by_val(p_xml,'XSourceValue','NTIS')),1,5);
            r_phys_row.zip2 := substr(get_tag_value(p_xml,'ZipPostalCode',get_tag_pos_by_val(p_xml,'XSourceValue','NTIS')),6,4);
         end if;

         if r_phys_row.last is null then --CQ27096
         save_msg(1
                  ,LABEL_WARNING
                  ,'Last Name value from physician is not present'|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
                  ,r_phys_row.npi_id --NPI_ID
                  ,NULL --PHYSICIAN_ID
                  ,r_phys_row.dea_id --DEA_ID
                  ,p_response_id --AIMI_HDR_ID
                  ,g_process_name);
         end if;
         r_phys_row.creation_date:=sysdate;
         r_phys_row.created_by:=user;
         r_phys_row.status:='A';
         r_phys_row.row_used := 0;

         INSERT INTO THOT.MDM_PHYSICIAN_RESPONSE VALUES R_PHYS_ROW;
         
         -- added by Carlos Rasso/CRojas CQ27616 [
         aux_addr1 := upper(get_tag_value(p_xml,'AddressLineOne',p));
         aux_addr2 := upper(get_tag_value(p_xml,'AddressLineTwo',p));
         aux_addrt := trim(aux_addr1) || ' ' || trim(aux_addr2);

         -- if the whole address have can not be placed in addr1
         IF LENGTH(aux_addrt) > 25 THEN
           -- look for the first space before position 26
           end_of_addr := 26;
           while substr(aux_addrt,end_of_addr,1) <> ' '
           loop
             end_of_addr := end_of_addr - 1;
           END loop;
           -- in addr1 will be written the complete words that finish before position 26
           aux_addr1 := substr(aux_addrt,1,end_of_addr - 1);
           -- addrt is the remaining text
           aux_addrt := substr(aux_addrt,end_of_addr + 1, LENGTH(aux_addrt));
           --if the remaining text can not be placed in addr2
           IF LENGTH(aux_addrt) > 25 THEN
             -- look for the firs space before position 26
             end_of_addr := 26;
             while substr(aux_addrt,end_of_addr,1) <> ' '
             loop
               end_of_addr := end_of_addr - 1;
             END loop;
             -- in addr1 will be written the complete words that finish before before position 26
             aux_addr2 := substr(aux_addrt,1,end_of_addr - 1);
             -- addrt is the remaining text
             aux_addrt := substr(aux_addrt,end_of_addr + 1, LENGTH(aux_addrt));
             --if there is a remaining text
             IF LENGTH(aux_addrt) > 0 THEN
               --log message := 'TEXT '''||aux_addrt||''' TRUNCATED'
               save_msg(1
                        ,LABEL_INFO
                        ,'MDM Address text: -'||aux_addrt||'- was truncated; THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
                        ,r_phys_row.npi_id --NPI_ID
                        ,NULL --PHYSICIAN_ID
                        ,r_phys_row.dea_id  --DEA_ID
                        ,p_response_id --AIMI_HDR_ID
                        ,g_process_name);
             END IF;
           ELSE
             --the whole remaning text can be placed in addr2
             aux_addr2 := TRIM(aux_addrt);
           END IF;
         ELSE
           --the whole text can be placed in addr2
           aux_addr1 := aux_addrt;
           aux_addr2 := NULL;
         END IF;

         r_addresses.addr1 := aux_addr1;
         R_ADDRESSES.ADDR2 := AUX_ADDR2;
         --] CRASSO/CROJAS  CQ27616


         --DEA Record
         IF R_PHYS_ROW.DEA_ID IS NOT NULL THEN         
            R_PHYS_DEA.DEA_NUMBER := R_PHYS_ROW.DEA_ID;
            r_phys_dea.expired_flag := 'N';
            R_PHYS_DEA.PHYS_ROW := P+1; 
            R_PHYS_DEA.BUSINESS_ACTIVITY_CODE := 'C'; --Temp MDM to provide mapping
            R_PHYS_DEA.SCHEDULE1 := NVL(GET_TAG_VALUE(P_XML,'Schedule1Value',P),V_NULL);
            R_PHYS_DEA.SCHEDULE2 := NVL(GET_TAG_VALUE(P_XML,'Schedule2Value',P),V_NULL);
            R_PHYS_DEA.SCHEDULE2N := NVL(GET_TAG_VALUE(P_XML,'Schedule2NValue',P),V_NULL);
            R_PHYS_DEA.SCHEDULE3 := NVL(GET_TAG_VALUE(P_XML,'Schedule3Value',P),V_NULL);
            R_PHYS_DEA.SCHEDULE3N := NVL(GET_TAG_VALUE(P_XML,'Schedule3NValue',P),V_NULL);
            R_PHYS_DEA.SCHEDULE4 := NVL(GET_TAG_VALUE(P_XML,'Schedule4Value',P),V_NULL);
            R_PHYS_DEA.SCHEDULE5 := NVL(GET_TAG_VALUE(P_XML,'Schedule5Value',P),V_NULL);
            R_PHYS_DEA.EXPIRATION_DATE := NVL(TO_DATE(SUBSTR(GET_TAG_VALUE(P_XML,'IdentificationExpiryDate',P),1,10),'YYYY-MM-DD'),to_Date('01,01,01','YYYY-MM-DD'));
            R_PHYS_DEA.SCHEDULEL1 := NVL(GET_TAG_VALUE(P_XML,'ScheduleL1Value',P),V_NULL);
            R_PHYS_DEA.CREATED_BY := USER; --Added by CRasso/CRojas CQ27651
            R_PHYS_DEA.CREATION_DATE := SYSDATE; --Added by CRasso/CRojas CQ27651

            --For physician main address, go look for XSourceValue = NTIS
                     
            R_PHYS_DEA.CITY := NVL(UPPER(GET_TAG_VALUE(P_XML,'City',GET_TAG_POS_BY_VAL(P_XML,'XSourceValue','NTIS'))),R_PHYS_ROW.CITY);-- Modified by Carlos Rasso CQ27528 dea.city for npi.city
            R_PHYS_DEA.STATE := NVL(UPPER(GET_TAG_VALUE(P_XML,'ProvinceStateValue',GET_TAG_POS_BY_VAL(P_XML,'XSourceValue','NTIS'))),R_PHYS_ROW.STATE); -- Modified by Carlos Rasso CQ27528 dea.state for npi.state
            r_phys_dea.zip_code := nvl(substr(get_tag_value(p_xml,'ZipPostalCode',get_tag_pos_by_val(p_xml,'XSourceValue','NTIS')),1,5),r_phys_row.zip1); -- Modified by Carlos Rasso CQ27528 dea.zip for npi.zip
            
            R_PHYS_DEA.NAME := R_PHYS_ROW.LAST||', '||R_PHYS_ROW.FIRST;

            R_PHYS_DEA.ID_REQUEST := P_RESPONSE_ID;
            begin
               insert into thot.mdm_physician_dea_resp values r_phys_dea;
            EXCEPTION WHEN OTHERS THEN

            save_msg(1
               ,LABEL_ERROR
               ,'Unexpected error while inserting DEA staging data: '||sqlerrm|| '; THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
               ,r_phys_row.npi_id --NPI_ID
               ,NULL --PHYSICIAN_ID
               ,r_phys_row.dea_id --DEA_ID
               ,p_response_id	 --AIMI_HDR_ID
               ,g_process_name);
            end;
             --Addresses end
         end if;
      end loop;

      --Licenses
      v_occur := get_occurrences_by_tag_val(p_xml,'IdentificationValue','STLC');
      for p in 0..v_occur-1 loop
         r_attributes_lic.id_request := p_response_id;
         r_attributes_lic.attribute := get_tag_value(p_xml,'IdentificationNumber',get_tag_pos_by_val(p_xml,'IdentificationValue','STLC'));
         r_attributes_lic.name_type := 'D';
         r_attributes_lic.expiration_date := to_date(substr(get_tag_value(p_xml,'IdentificationExpiryDate',get_tag_pos_by_val(p_xml,'IdentificationValue','STLC')),1,10),'YYYY-MM-DD');
         r_attributes_lic.name_id := 1;
         r_attributes_lic.attribute_type := '08 ST LICENSE';
         r_attributes_lic.uniq_id := 1;
         r_attributes_lic.status := 'A';
         r_attributes_lic.state := upper(get_tag_value(p_xml,'ProvinceStateValue',get_tag_pos_by_val(p_xml,'XSourceValue','STLC'))); --upper(get_tag_value(p_xml,'ProvinceStateValue',0));
         if r_attributes_lic.attribute is not null then
            begin
               insert into thot.mdm_physician_attr_resp values r_attributes_lic;
            exception
            when others then
            save_msg(1
               ,LABEL_ERROR
               ,'Unexpected error while inserting STATE LICENSES staging data: '||sqlerrm|| '; THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
               ,r_phys_row.npi_id --NPI_ID
               ,NULL --PHYSICIAN_ID
               ,r_phys_row.dea_id  --DEA_ID
               ,p_response_id --AIMI_HDR_ID
               ,g_process_name);
            end;
         end if;
      end loop;
      --Addresses


  -- added by Carlos Rasso CQ27587 Begin[
  --TCRMAPartyAddressBObj
    V_OCCUR := GET_OCCURRENCES_BY_TAG(P_XML,'TCRMPartyAddressBObj');

    FOR R IN 0..V_OCCUR-1 LOOP

      -- ] End added by Carlos Rasso CQ27587
      --TCRMAddressBObj
      V_OCCUR := GET_OCCURRENCES_BY_TAG(P_XML,'TCRMAddressBObj');
      V_COUNT_ADDR := 0;                          --++ added by CQ27497 JGonzalez Mar-11-2015
      v_addr_seq1_found :=0;                      --++ added by CQ27587 JGonzalez Mar-31-2015
--      r_addresses.addr_seq := v_addr_init_seq;  -- commented by CQ27497 JGonzalez Mar-11-2015
      FOR P IN 0..V_OCCUR-1 LOOP
         v_count_addr := v_count_addr + 1;        --++ added by CQ27497 JGonzalez Mar-11-2015
         r_addresses.id_request := p_response_id;
         r_addresses.addr_effective_date := sysdate;
         r_addresses.status := 'A';
         r_addresses.NAME := r_phys_row.FIRST||' '||r_phys_row.LAST;  --CQ27223
         r_addresses.attn := r_phys_row.LAST||', '||r_phys_row.FIRST;   --++ added by CQ27480 Jose Gonzalez Mar-06-2015
         r_addresses.name_id := 1;
         R_ADDRESSES.NAME_TYPE := 'D';
         R_ADDRESSES.ID_VALUE := GET_TAG_VALUE (P_XML,'AddressId',P); --Added by CRasso/CRojas CQ27651

         IF GET_TAG_VALUE(P_XML,'XSourceValue',P) = 'NTIS' THEN   --DEA
--            R_ADDRESSES.MDM_ADDR_ID := GET_TAG_VALUE(P_XML,'IdentificationNumber',GET_OCCUR_INDEX_BY_TAG_VAL(P_XML,'IdentificationValue','DEA',P));--get_tag_value(p_xml,'IdentificationNumber',p);  -- commented by JOSE GONZALEZ CQ27616 APR-02-2015
            R_ADDRESSES.DEA_OR_NPI  := 'DEA';  -- added by JOSE GONZALEZ CQ27616 APR-02-2015
            R_ADDRESSES.MDM_ADDR_ID := GET_TAG_VALUE(P_XML,'CareOf',p-nu_npi_addrs);  -- added by JOSE GONZALEZ CQ27616 APR-02-2015
         ELSIF GET_TAG_VALUE(P_XML,'XSourceValue',P) = 'CMS' THEN --NPIs
            nu_npi_addrs := nu_npi_addrs + 1; -- added by JOSE GONZALEZ CQ27616 APR-02-2015
          --  r_addresses.mdm_addr_id := get_tag_value(p_xml,'IdentificationNumber',get_occur_index_by_tag_val(p_xml,'IdentificationValue','NPI',p));--get_tag_value(p_xml,'IdentificationNumber',p);
            R_ADDRESSES.DEA_OR_NPI := 'NPI';  -- added by JOSE GONZALEZ CQ27616 APR-02-2015
            R_ADDRESSES.MDM_ADDR_ID:=GET_TAG_VALUE(P_XML,'IdentificationNumber',GET_TAG_POS_BY_VAL(P_XML,'IdentificationValue','NPI'));
           
         END IF;
         
         

--         R_ADDRESSES.DEA_OR_NPI := R_ADDRESSES.MDM_ADDR_ID; --added by Carlos Rasso CQ27587 populating dea_or_npi value in table -- commented by JOSE GONZALEZ CQ27616 APR-02-2015
         R_ADDRESSES.DEA_OR_NPI_VAL := R_ADDRESSES.MDM_ADDR_ID; -- added by JOSE GONZALEZ CQ27616 APR-02-2015

         if instr(upper(get_tag_value(p_xml,'AddressUsageValue',p)),'OFFICE') > 0 then
            r_addresses.address_type := 'OFFICE';
            -- CQ27497 JGonzalez Mar-11-2015 [
            IF v_addr_seq1_found = 0 THEN
              r_addresses.addr_seq     := 1;
              v_addr_seq1_found        := 1;
            ELSE
              r_addresses.addr_seq     := v_addr_init_seq + v_count_addr - v_addr_seq1_found;
            END IF;
            -- ] CQ27497 JGonzalez Mar-11-2015
         else
            r_addresses.address_type := 'OTHER';
            --r_addresses.addr_seq     := r_addresses.addr_seq+1;                            -- commented by CQ27497 JGonzalez Mar-11-2015
            r_addresses.addr_seq     := v_addr_init_seq + v_count_addr - v_addr_seq1_found;  --++ added by CQ27497 JGonzalez Mar-11-2015
         END IF;

         -- added by Carlos Rasso CQ27528 [
         aux_addr1 := upper(get_tag_value(p_xml,'AddressLineOne',p));
         aux_addr2 := upper(get_tag_value(p_xml,'AddressLineTwo',p));
         aux_addrt := trim(aux_addr1) || ' ' || trim(aux_addr2);

         -- if the whole address have can not be placed in addr1
         IF LENGTH(aux_addrt) > 25 THEN
           -- look for the first space before position 26
           end_of_addr := 26;
           while substr(aux_addrt,end_of_addr,1) <> ' '
           loop
             end_of_addr := end_of_addr - 1;
           END loop;
           -- in addr1 will be written the complete words that finish before position 26
           aux_addr1 := substr(aux_addrt,1,end_of_addr - 1);
           -- addrt is the remaining text
           aux_addrt := substr(aux_addrt,end_of_addr + 1, LENGTH(aux_addrt));
           --if the remaining text can not be placed in addr2
           IF LENGTH(aux_addrt) > 25 THEN
             -- look for the firs space before position 26
             end_of_addr := 26;
             while substr(aux_addrt,end_of_addr,1) <> ' '
             loop
               end_of_addr := end_of_addr - 1;
             END loop;
             -- in addr1 will be written the complete words that finish before before position 26
             aux_addr2 := substr(aux_addrt,1,end_of_addr - 1);
             -- addrt is the remaining text
             aux_addrt := substr(aux_addrt,end_of_addr + 1, LENGTH(aux_addrt));
             --if there is a remaining text
             IF LENGTH(aux_addrt) > 0 THEN
               --log message := 'TEXT '''||aux_addrt||''' TRUNCATED'
               save_msg(1
                        ,LABEL_INFO
                        ,'MDM Address text: -'||aux_addrt||'- was truncated; THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
                        ,r_phys_row.npi_id --NPI_ID
                        ,NULL --PHYSICIAN_ID
                        ,r_phys_row.dea_id  --DEA_ID
                        ,p_response_id --AIMI_HDR_ID
                        ,g_process_name);
             END IF;
           ELSE
             --the whole remaning text can be placed in addr2
             aux_addr2 := TRIM(aux_addrt);
           END IF;
         ELSE
           --the whole text can be placed in addr2
           aux_addr1 := aux_addrt;
           aux_addr2 := NULL;
         END IF;

         r_addresses.addr1 := aux_addr1;
         R_ADDRESSES.ADDR2 := AUX_ADDR2;
         -- ] added Carlos Rasso by CQ27528

--         r_addresses.addr1 := trim(substr(upper(get_tag_value(p_xml,'AddressLineOne',p)),1,25));  --commented by Carlos Rasso CQ27528
--         r_addresses.addr2 := trim(substr(upper(get_tag_value(p_xml,'AddressLineTwo',p)),1,25));  --commented by Carlos Rasso CQ27528
         r_addresses.city := upper(get_tag_value(p_xml,'City',p));
         r_addresses.state := upper(get_tag_value(p_xml,'ProvinceStateValue',p));
         r_addresses.country := 'USA';
         if length(get_tag_value(p_xml,'ZipPostalCode',p))>5 then
            r_addresses.zip := substr(get_tag_value(p_xml,'ZipPostalCode',p),1,5)||'-'||substr(get_tag_value(p_xml,'ZipPostalCode',p),6,4);
         else
            r_addresses.zip := get_tag_value(p_xml,'ZipPostalCode',p);
         end if;
         r_addresses.creation_date:=sysdate;
         r_addresses.created_by:=user;

--         if v_mdm_addr_type = 'NPI' then  --CQ27096                   -- commented by JOSE GONZALEZ CQ27616 APR-02-2015
--            R_ADDRESSES.MDM_ADDR_ID := R_PHYS_ROW.NPI_ID;             -- commented by JOSE GONZALEZ CQ27616 APR-02-2015
--             ELSE  R_ADDRESSES.MDM_ADDR_ID := R_PHYS_ROW.DEA_ID;      -- commented by JOSE GONZALEZ CQ27616 APR-02-2015
--         END IF;                                                      -- commented by JOSE GONZALEZ CQ27616 APR-02-2015

          BEGIN
            V_FOUND := -1;                   --++ added by CQ27587 JGonzalez Mar-31-2015

            -- CQ27497 JGonzalez Mar-11-2015 [
            SELECT count(*)
              INTO v_found
              FROM thot.mdm_physician_addr_resp A
             WHERE A.name_type        = r_addresses.name_type
               AND A.name_id          = r_addresses.name_id
               AND A.address_type     = r_addresses.address_type
               AND A.id_request       = r_addresses.id_request
               AND A.addr1            = r_addresses.addr1
               AND nvl(A.addr2,'1')   = nvl(r_addresses.addr2,'1')
               AND nvl(A.city,'1')    = nvl(r_addresses.city,'1')
               AND nvl(A.state,'1')   = nvl(r_addresses.state,'1')
               AND nvl(A.country,'1') = nvl(r_addresses.country,'1')
               AND nvl(A.ZIP,'1')     = nvl(R_ADDRESSES.ZIP,'1') 
               AND a.dea_or_npi_val   = nvl(r_addresses.dea_or_npi_val,a.dea_or_npi_val);
               
               -- jcorona  in the transactional table we will insert all addresses comming from the response 
               -- and we will have the duplicate validation in the actual insert to RxHome 
               
            --added by CQ27616 code will not insert duplicated or not valid DEA
           IF v_found = 0 --and  (GET_TAG_VALUE(P_XML,'XSourceValue',P) = 'CMS' or r_addresses.mdm_addr_id = R_ADDRESSES.dea_or_npi) -- modified by JOSE GONZALEZ CQ27616 APR-02-2015
           THEN
            -- ] CQ27497 JGonzalez Mar-11-2015

              INSERT INTO thot.mdm_physician_addr_resp VALUES r_addresses ;
            -- CQ27497 JGonzalez Mar-11-2015 [

            ELSE
             save_msg(1
                  ,LABEL_INFO
                  ,'MDM duplicated or is not valid DEA address was not taken in count: ID_REQUEST='||r_addresses.id_request||', ADDR1='||r_addresses.addr1||', ADDR2='||r_addresses.addr2||', CITY='||r_addresses.city||', STATE='||r_addresses.state||', ZIP='||r_addresses.zip||'; THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
                  ,r_phys_row.npi_id --NPI_ID
                  ,NULL --PHYSICIAN_ID
                  ,r_phys_row.dea_id  --DEA_ID
                  ,p_response_id --AIMI_HDR_ID
                  ,G_PROCESS_NAME);
            END IF;
            -- ] CQ27497 JGonzalez Mar-11-2015
          exception
          when others then
             save_msg(1
                  ,LABEL_ERROR
                  ,'Unexpected error while inserting ADDRESSES staging data: '||sqlerrm|| '; THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
                  ,r_phys_row.npi_id --NPI_ID
                  ,NULL --PHYSICIAN_ID
                  ,r_phys_row.dea_id  --DEA_ID
                  ,p_response_id --AIMI_HDR_ID
                  ,g_process_name);
          end;
      END LOOP;
  END LOOP; --++ added by CQ27587 Carlos Rasso
  
      --JGONZALEZ CQ27616 APR-03-2015 [
    --Update mdm_phys_dea_resp with the minimum addr for each DEA in mdm_phys_addr_resp
    MERGE INTO 
      (SELECT A.id_request, A.dea_number, a.address1, A.address2, A.city, a.state, A.zip_code,A.creation_date, A.created_by --CQ27651 creation_date and created_by added
         FROM thot.mdm_physician_dea_resp A
        WHERE A.id_request = p_response_id) dea 
      USING 
        (SELECT b.id_request, b.dea_or_npi_val, b.addr1, b.addr2, b.city, b.state, b.zip, b.creation_date, b.created_by --CQ27651 creation_date and created_by added
           FROM thot.mdm_physician_addr_resp b
          WHERE b.id_request = p_response_id
            AND b.dea_or_npi = 'DEA'
            AND b.addr_seq   = (SELECT MIN(addr_seq) 
                                  FROM thot.mdm_physician_addr_resp c 
                                 WHERE c.id_request = p_response_id
                                   AND c.dea_or_npi = 'DEA' 
                                   AND C.DEA_OR_NPI_VAL = B.DEA_OR_NPI_VAL)
        ) addr 
      ON (dea.id_request     = addr.id_request 
          AND dea.dea_number = addr.dea_or_npi_val)
      WHEN MATCHED THEN 
        UPDATE SET
          DEA.ADDRESS1        = ADDR.ADDR1 
          , dea.address2      = addr.addr2
          , DEA.CITY          = ADDR.CITY
          , DEA.STATE         = ADDR.STATE
          , DEA.ZIP_CODE      = SUBSTR(ADDR.ZIP,1,5)
          , DEA.CREATION_DATE = ADDR.CREATION_DATE
          , DEA.CREATED_BY    = ADDR.CREATED_BY
    ;
    --] JGONZALEZ CQ27616 APR-03-2015


      --Phones
      v_occur := get_occurrences_by_tag(p_xml,'ContactMethodValue');
      if v_occur > 0 then
         for i in 0..v_occur-1 loop
            if instr(upper(get_tag_value(p_xml,'ContactMethodValue',i)),'EMAIL') = 0 then
               v_phone_str_aux := null;
               r_phones.name_id := 1;
               r_phones.name_type:= 'D';
               r_phones.addr_seq:= 1;
               --r_phones.phone_seq := i+1;

               if (instr(upper(get_tag_value(p_xml,'ContactMethodValue',i)),'OFFICE') > 0 or
                  instr(upper(get_tag_value(p_xml,'ContactMethodUsageValue',i)),'OFFICE') > 0 ) and instr(nvl(v_phones_types_ins,'x'),'OFFICE') = 0 then
                  r_phones.phone_seq := 1;
                  r_phones.phone_type := 'OFFICE';
               elsif (instr(upper(get_tag_value(p_xml,'ContactMethodValue',i)),'FAX') > 0 or
                  instr(upper(get_tag_value(p_xml,'ContactMethodUsageValue',i)),'FAX') > 0) and instr(nvl(v_phones_types_ins,'x'),'FAX') = 0 then
                  r_phones.phone_seq := 2;
                  r_phones.phone_type := 'FAX';
               else
                  v_phone_seq_oth := v_phone_seq_oth + 1;
                  r_phones.phone_seq := v_phone_seq_oth;
                  r_phones.phone_type := 'OTHER';
               end if;
                  v_phones_types_ins := v_phones_types_ins||r_phones.phone_type||','; --to avoid repeat OFFICE or FAX, first occurence [[Assumption]]
                  r_phones.id_request := p_response_id;

                  v_phone_str_aux := get_tag_value(p_xml,'ReferenceNumber',i);
                  r_phones.phonea := regexp_substr(regexp_replace(v_phone_str_aux,'(\(|\))','') ,'[0-9]{3}',1);
                  r_phones.phoneb := regexp_substr(regexp_replace(v_phone_str_aux,'(\(|\))','') ,'[0-9]{3}',2);
                  r_phones.phonec := regexp_substr(regexp_replace(v_phone_str_aux,'(\(|\))','') ,'[0-9]{4}',1);
                  r_phones.creation_date := sysdate;
                  r_phones.created_by := user;

                  if r_phones.phone_type = 'FAX' then
                     BEGIN

                        SELECT count(*)
                          INTO v_found
                          FROM thot.mdm_physician_phones_resp A
                         WHERE A.name_type      = r_phones.name_type
                           AND A.name_id        = r_phones.name_id
                           AND A.phone_type     in ('FAX','AUTOFAX')
                           AND A.id_request     = r_phones.id_request
                           AND A.phonea         = r_phones.phonea
                           AND A.phoneb         = r_phones.phoneb
                           AND A.phonec         = r_phones.phonec
                        ;
                        IF v_found = 0 THEN
                        -- ] CQ27497 JGonzalez Mar-11-2015
                          INSERT INTO thot.mdm_physician_phones_resp VALUES r_phones;
                          r_phones.phone_seq := 3;
                          r_phones.phone_type := 'AUTOFAX';
                          INSERT INTO thot.mdm_physician_phones_resp VALUES r_phones;
                        -- CQ27497 JGonzalez Mar-11-2015 [
                        ELSE
                          save_msg(1
                                   ,LABEL_INFO
                                   ,'MDM duplicated phone was not taken in count: ID_REQUEST='||r_phones.id_request||', PHONE_TYPE='||r_phones.phone_type||', PHONE_A='||r_phones.phonea||', PHONE_B='||r_phones.phoneb||', PHONE_C='||r_phones.phonec||'; THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
                                   ,r_phys_row.npi_id --NPI_ID
                                   ,NULL --PHYSICIAN_ID
                                   ,r_phys_row.dea_id  --DEA_ID
						  				             ,p_response_id --AIMI_HDR_ID
                                   ,g_process_name);
                        END IF;
                        -- ] CQ27497 JGonzalez Mar-11-2015
                     exception
                     when others then
                        save_msg(1
                                 ,LABEL_ERROR
                                 ,'Unexpected error while inserting PHONES staging data: '||sqlerrm|| '; THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
                                 ,r_phys_row.npi_id --NPI_ID
                                 ,NULL --PHYSICIAN_ID
                                 ,r_phys_row.dea_id  --DEA_ID
										             ,p_response_id --AIMI_HDR_ID
                                 ,g_process_name);
                     end;
                  else
                     BEGIN
                        -- CQ27497 JGonzalez Mar-11-2015 [
                        SELECT count(*)
                          INTO v_found
                          FROM thot.mdm_physician_phones_resp A
                         WHERE A.name_type      = r_phones.name_type
                           AND A.name_id        = r_phones.name_id
                           AND A.phone_type     = decode(r_phones.phone_type,'OTHER',A.phone_type,r_phones.phone_type)
                           AND A.id_request     = r_phones.id_request
                           AND A.phonea         = r_phones.phonea
                           AND A.phoneb         = r_phones.phoneb
                           AND A.phonec         = r_phones.phonec
                        ;
                        IF v_found = 0 THEN
                        -- ] CQ27497 JGonzalez Mar-11-2015
                          INSERT INTO thot.mdm_physician_phones_resp VALUES r_phones;
                        -- CQ27497 JGonzalez Mar-11-2015 [
                        ELSE
                          save_msg(1
                                   ,LABEL_INFO
                                   ,'MDM duplicated phone was not taken in count: ID_REQUEST='||r_phones.id_request||', PHONE_TYPE='||r_phones.phone_type||', PHONE_A='||r_phones.phonea||', PHONE_B='||r_phones.phoneb||', PHONE_C='||r_phones.phonec||'; THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
                                   ,r_phys_row.npi_id --NPI_ID
                                   ,NULL --PHYSICIAN_ID
                                   ,r_phys_row.dea_id  --DEA_ID
						  				             ,p_response_id --AIMI_HDR_ID
                                   ,g_process_name);
                        END IF;
                        -- ] CQ27497 JGonzalez Mar-11-2015
                     exception
                     when others then
                        save_msg(1
                              ,LABEL_ERROR
                              ,'Unexpected error while inserting PHONES staging data: '||sqlerrm|| '; THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
                              ,r_phys_row.npi_id --NPI_ID
                              ,NULL --PHYSICIAN_ID
                              ,r_phys_row.dea_id  --DEA_ID
										          ,p_response_id --AIMI_HDR_ID
                              ,g_process_name);
                     end;
                  end if;
            end if;
         end loop;
      end if;

      commit;

      save_msg(1
               ,LABEL_INFO
               ,'End response parsing.'|| ' THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
               ,r_phys_row.npi_id --NPI_ID
               ,NULL --PHYSICIAN_ID
               ,r_phys_row.dea_id  --DEA_ID
					,p_response_id --AIMI_HDR_ID
               ,g_process_name);
   exception
   when others then
      save_msg(1
               ,LABEL_ERROR
               ,'Unexpected error while parsing the xml response AIMI HDR: ' || p_response_id || ' - ' || sqlerrm|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.parse_phys_mdm_response'
               ,r_phys_row.npi_id --NPI_ID
               ,NULL --PHYSICIAN_ID
               ,r_phys_row.dea_id  --DEA_ID
					,p_response_id --AIMI_HDR_ID
               ,g_process_name);
   end parse_phys_mdm_response;

-- +============================================================================================================+
-- |   Name: get_tag_pos_by_val                                                                                 |
-- |   Description: This function returns tag position, searching for tag name using dbms_xmldom library        |
-- |   Parameters: p_xml     : Incoming XML response                                                            |
-- |               p_tag      : Tag name                                                                        |
-- |               p_search_val      : The actual value within the tag                                          |
-- |   Returns:    postition of the tag                                                                         |
-- |   Author: Diego Resendi CQ27045                                                                            |
-- +============================================================================================================+
   function get_tag_pos_by_val(p_xml         clob
                              ,p_tag         varchar2
                              ,p_search_val  varchar2
                              ,p_pos_val     number default null) return number
   is
      v_doc dbms_xmldom.domdocument;
      v_nodelist_parent dbms_xmldom.domnodelist;
      v_node_parent dbms_xmldom.domnode;
      v_tag_value_parent varchar2(1000):=null;
      v_parent_node_len  number:=null;
      v_node_pos  number:=null;
   begin
      v_doc := dbms_xmldom.newdomdocument(p_xml);
      v_nodelist_parent := dbms_xmldom.getelementsbytagname(v_doc, p_tag);

        v_parent_node_len := DBMS_XMLDOM.GetLength(v_nodelist_parent);
        for i in 0 .. v_parent_node_len - 1 loop

            v_node_parent := dbms_xmldom.getfirstchild(dbms_xmldom.item(v_nodelist_parent, i));
            v_tag_value_parent := DBMS_XMLDOM.GetNodeValue(v_node_parent);
               /*if lower(v_tag_value_parent) = lower(p_search_val) then
                  v_node_pos := i;
               end if;*/
               if p_pos_val is not null and i != p_pos_val and p_pos_val != 0 then
                  if lower(v_tag_value_parent) = lower(p_search_val) then
                     v_node_pos := i;
                     exit;
                  end if;
               else
                  if lower(v_tag_value_parent) = lower(p_search_val) then
                     v_node_pos := i;
                     exit;
                  end if;
               end if;
        end loop;
   return v_node_pos;
   exception
      when others then
      return null;
   end get_tag_pos_by_val;

-- +============================================================================================================+
-- |   Name: get_tag_value                                                                                      |
-- |   Description: This function returns tag value, searching for tag name and position                        |
-- |   Parameters: p_xml     : Incoming XML response                                                            |
-- |               p_tag      : Tag name                                                                        |
-- |               p_pos      : the actual position                                                             |
-- |   Returns:    value of the tag                                                                             |
-- |   Author: Diego Resendi CQ27045                                                                            |
-- +============================================================================================================+
   function get_tag_value(p_xml          clob
                           ,p_tag   varchar2
                           ,p_pos   varchar2 default null) return varchar2
   is
      v_doc dbms_xmldom.domdocument;
      v_nodelist_parent dbms_xmldom.domnodelist;
      v_node_parent dbms_xmldom.domnode;
      v_tag_value_parent varchar2(1000):=null;
      v_sql_error varchar2(1000);
   begin
      if p_pos is not null then
         v_doc := dbms_xmldom.newdomdocument(p_xml);
         v_nodelist_parent := dbms_xmldom.getelementsbytagname(v_doc, p_tag);
         --v_nodelist_parent := dbms_xslprocessor.selectnodes(dbms_xmldom.makenode(v_doc),
           --                                    '/shippingMessageResponse');

         v_node_parent := dbms_xmldom.getfirstchild(dbms_xmldom.item(v_nodelist_parent, p_pos));
         v_tag_value_parent := DBMS_XMLDOM.GetNodeValue(v_node_parent);
         return v_tag_value_parent;
      else
         return null;
      end if;

   exception
      when others then
      v_sql_error := sqlerrm;
      return null;
   end get_tag_value;

-- +============================================================================================================+
-- |   Name: get_occurrences_by_tag                                                                             |
-- |   Description: This function returns tag ocurrences, searching for tag name using dbms_xmldom library      |
-- |   Parameters: p_xml     : Incoming XML repsonse                                                            |
-- |               p_tag      : Tag name                                                                        |
-- |   Returns:    number of ocurences of the tag                                                               |
-- |   Author: Diego Resendi CQ27045                                                                            |
-- +============================================================================================================+
   function get_occurrences_by_tag(p_xml         clob
                                 ,p_tag         varchar2 ) return number
   is
      v_doc dbms_xmldom.domdocument;
      v_nodelist_parent dbms_xmldom.domnodelist;
      v_node_parent dbms_xmldom.domnode;
      v_tag_value_parent varchar2(1000):=null;
      v_parent_node_len  number:=null;
   begin
      v_doc := dbms_xmldom.newdomdocument(p_xml);
      v_nodelist_parent := dbms_xmldom.getelementsbytagname(v_doc, p_tag);

      v_parent_node_len := DBMS_XMLDOM.GetLength(v_nodelist_parent);
      return v_parent_node_len;

   exception
   when others then
      return null;
   end get_occurrences_by_tag;

-- +=============================================================================================================+
-- |   Name: parse_and_validate_found                                                                            |
-- |   Description: This Procedure will parse ans look if no physician is found upon response                    |
-- |   Parameters: p_response_id : Id that identifies the WS response. Aimi_Hdr_ID                               |
-- |   Returns: new status RESP_PHYSICIAN_NOT_FOUND or null                                                      |
-- |   Author: Diego Resendi CQ27045                                                                             |
-- +============================================================================================================+
   function parse_and_validate_found(p_response_id number) return varchar2
   is
      v_xml clob;
      v_error_message   varchar2(1000);
      v_result_code     varchar2(200);

   begin

      select response_xml
      into v_xml
      from rxh_custom.web_services_trans
      where id = p_response_id;

      v_error_message := upper(get_tag_value(v_xml,'ErrorMessage',0));
      v_result_code := upper(get_tag_value(v_xml,'ResultCode',0));

      if instr(lower(v_error_message),lower('Identifier entered is not in MDM')) > 0 and upper(v_result_code) = 'FATAL' then
         return 'PHYS_NOT_FOUND';
      else
         return null;
      end if;

   exception
   when others then
      save_msg(1
               ,LABEL_ERROR
               ,'Unexpected error while parsing the xml response '||sqlerrm|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.parse_and_validate_found'
               ,NULL --NPI_ID
               ,NULL --PHYSICIAN_ID
               ,NULL --DEA_ID
					,p_response_id --AIMI_HDR_ID
               ,g_process_name);
      return null;
   end parse_and_validate_found;

-- +============================================================================================================+
-- |   Name: get_occurrences_by_tag_val                                                                             |
-- |   Description: This function returns tag ocurrences, searching for tag name using dbms_xmldom library      |
-- |   Parameters: p_xml     : Incoming XML repsonse                                                            |
-- |               p_tag      : Tag name                                                                        |
-- |               p_val       : Value to look for
-- |   Returns:    number of ocurences of the tag                                                               |
-- |   Author: Diego Resendi CQ27045                                                                            |
-- +============================================================================================================+
   function get_occurrences_by_tag_val(p_xml         clob
                                       ,p_tag        varchar2
                                       ,p_val        varchar2) return number
   is
      v_doc dbms_xmldom.domdocument;
      v_nodelist_parent dbms_xmldom.domnodelist;
      v_node_parent dbms_xmldom.domnode;
      v_tag_value_parent varchar2(1000):=null;
      v_parent_node_len  number:=null;
      v_occur number:=0;
      v_new_occur number:=0;
   begin
      v_doc := dbms_xmldom.newdomdocument(p_xml);
      v_occur:=thot.physicians_clean_up_pkg.get_occurrences_by_tag(p_xml,p_tag);
      v_nodelist_parent := dbms_xmldom.getelementsbytagname(v_doc, p_tag);
      for i in 0..v_occur loop
         v_node_parent := dbms_xmldom.getfirstchild(dbms_xmldom.item(v_nodelist_parent, i));
         v_tag_value_parent := DBMS_XMLDOM.GetNodeValue(v_node_parent);
         if v_tag_value_parent = p_val then
            v_new_occur := v_new_occur + 1;
         end if;
         v_nodelist_parent := dbms_xmldom.getelementsbytagname(v_doc, p_tag);
      end loop;

      return v_new_occur;

   exception
   when others then
      return null;
   end get_occurrences_by_tag_val;
-- +============================================================================================================+
-- |   Name: update_phys_addresses                                                                             |
-- |   Description: This procedures updates(inserts) only addresses block      |
-- |   Author: Diego Resendi CQ27045                                                                            |
-- +============================================================================================================+
   procedure update_phys_addresses( p_physician_id  number
                                   ,P_REQUEST_ID   NUMBER
                                   ,p_dea_id varchar2 --++ added by CQ27587 Carlos Rasso
                                    ) is

   CURSOR C_MDM_SEARCH_ADDR IS
  /* select *
   FROM THOT.MDM_PHYSICIAN_ADDR_RESP
   WHERE ID_REQUEST     = P_REQUEST_ID
     and mdm_addr_id    = p_dea_id --++ added by CQ27587 Carlos Rasso
   order by addr_seq;}*/
   
    SELECT * --ADDED BY CQ27616 CRASSO/CROJAS
      FROM THOT.MDM_PHYSICIAN_ADDR_RESP 
     WHERE ID_REQUEST    = P_REQUEST_ID
       AND p_dea_id      = DECODE(DEA_OR_NPI,'NPI',p_dea_id,DEA_OR_NPI_VAL)
  ORDER BY address_type; -- JC

   cursor c_rxhome_addr is
   select *
   from thot.addresses
   where name_id = p_physician_id
   and name_type = 'D'
   order by addr_seq;

   r_mdm_phys_addr THOT.addresses%rowtype;
   --r_addr_temp   thot.addreses%rowtype;
   x_rowid                  ROWID;
   x_errors                 VARCHAR2(100);
   x_retcode                number;
   v_no_existing_addresses  number;
   
   V_STATE                 VARCHAR2(100):= NULL ; -- JC
   V_COUNT_SEQ             NUMBER := 0; -- JC
      
   BEGIN
      -- CQ27497 JGonzalez Mar-11-2015 [
      save_msg(1
               ,LABEL_INFO
               ,'Begin update physician addresses. THOT.PHYSICIANS_CLEAN_UP_PKG.update_phys_addresses'
               ,NULL --NPI_ID
               ,p_physician_id --PHYSICIAN_ID
               ,NULL --DEA_ID
               ,NULL --AIMI_HDR_ID
               ,g_process_name);
      -- ] CQ27497 JGonzalez Mar-11-2015
      select count(1)
      into v_no_existing_addresses
      from thot.addresses
      where name_id = p_physician_id
      and name_type = 'D';
      
        --JC START
                 BEGIN 
                   SELECT STATE
                     INTO v_state
                     FROM THOT.MDM_PHYSICIAN_ADDR_RESp 
                    WHERE ID_REQUEST = p_request_id
                      AND DEA_OR_NPI = 'DEA'
                      AND DEA_OR_NPI_VAL = p_dea_id
                      AND addr_seq = (SELECT min(addr_seq)
                                        FROM THOT.MDM_PHYSICIAN_ADDR_RESP P 
                                       WHERE P.ID_REQUEST =  p_request_id
                                         AND P.DEA_OR_NPI = 'DEA'
                                         AND P.DEA_OR_NPI_VAL = p_dea_id );
                 EXCEPTION 
                    WHEN NO_DATA_FOUND THEN 
                        v_state := NULL ; --> STATE WILL BE NULL THEREFORE NO ADDRESS WILL BE INSERTED
                 END ; 
            --JC END

      if v_no_existing_addresses = 0 then
      for i in c_mdm_search_addr loop
        IF v_state = i.state  AND is_phys_addr_exist(i.addr1,i.addr2,i.state,i.zip,p_physician_id , i.city  ) = 'N' THEN --JC
                 V_COUNT_SEQ  := V_COUNT_SEQ + 1 ;
      --insert new address, next available sequence
                 r_mdm_phys_addr.NAME_TYPE := i.name_type;
                 r_mdm_phys_addr.name_id := p_physician_id;
                 select nvl(max(addr_seq),0)+1
                 into r_mdm_phys_addr.ADDR_SEQ
                 from thot.addresses
                 where name_id = p_physician_id
                 and name_type = 'D';
                 --r_mdm_phys_addr.ADDR_SEQ := i.addr_seq;   --logic TBD
                 r_mdm_phys_addr.ATTN := i.attn;
                 r_mdm_phys_addr.NAME := i.NAME;
                 r_mdm_phys_addr.ADDR1 := i.addr1;
                 r_mdm_phys_addr.ADDR2 := i.addr2;
                 r_mdm_phys_addr.CITY := i.city;
                 r_mdm_phys_addr.STATE := i.state;
                 r_mdm_phys_addr.ZIP := i.zip;
                 r_mdm_phys_addr.COMMENTS := i.comments;
                 IF V_COUNT_SEQ  = 1 AND i.address_type != 'OFFICE' THEN
                    r_mdm_phys_addr.ADDRESS_TYPE := 'OFFICE';
                 ELSE
                    r_mdm_phys_addr.ADDRESS_TYPE := i.address_type;
                 END IF ; 
                 r_mdm_phys_addr.COUNTRY := i.country;
                 r_mdm_phys_addr.STATUS := i.status;
                 r_mdm_phys_addr.COUNTY := i.county;
                 r_mdm_phys_addr.COMPANY_NAME := i.company_name;
                 r_mdm_phys_addr.CONTACT_FIRST_NAME := i.contact_first_name;
                 r_mdm_phys_addr.CONTACT_LAST_NAME := i.contact_last_name;
                 r_mdm_phys_addr.PHY_CLINIC_FLAG := i.phy_clinic_flag;
                 r_mdm_phys_addr.MARKETING_ID := i.marketing_id;
                 r_mdm_phys_addr.ADDR_EFFECTIVE_DATE := i.addr_effective_date;

                 thot.addresses_pkg.insert_PRC(x_rowid
                                             , r_mdm_phys_addr
                                             , x_errors
                                             , x_retcode );
                  if x_retcode <> 0 then
                     null;   ---Log error
                     save_msg(1
                              ,LABEL_ERROR
                              ,'Unexpected error while inserting address information (loop)'||x_errors|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.update_phys_addresses'
                              ,NULL --NPI_ID
                              ,p_physician_id --PHYSICIAN_ID
                              ,NULL --DEA_ID
                              ,NULL --AIMI_HDR_ID
                              ,g_process_name);
                  end if;
         END IF; -- JC         
      end loop;
      else
      for i in c_mdm_search_addr loop
         --for j in c_rxhome_addr loop  --JC  this loop was commented 
            --if i.address_type = j.address_type and i.zip != j.zip then  --CQ27096
            IF v_state =  i.state THEN   --JC
               V_COUNT_SEQ  := V_COUNT_SEQ + 1 ;  --JC
            END IF ; --JC
            IF   v_state =  i.state  AND is_phys_addr_exist(i.addr1,i.addr2,i.state,i.zip,p_physician_id , i.city ) = 'N' THEN
                --insert new address, next available sequence
                 r_mdm_phys_addr.NAME_TYPE := i.name_type;
                 r_mdm_phys_addr.name_id := p_physician_id;
                 select nvl(max(addr_seq),0)+1
                 into r_mdm_phys_addr.ADDR_SEQ
                 from thot.addresses
                 where name_id = p_physician_id
                 and name_type = 'D';
                 --r_mdm_phys_addr.ADDR_SEQ := i.addr_seq;   --logic TBD
                 r_mdm_phys_addr.ATTN := i.attn;
                 r_mdm_phys_addr.NAME := i.NAME;
                 r_mdm_phys_addr.ADDR1 := i.addr1;
                 r_mdm_phys_addr.ADDR2 := i.addr2;
                 r_mdm_phys_addr.CITY := i.city;
                 r_mdm_phys_addr.STATE := i.state;
                 r_mdm_phys_addr.ZIP := i.zip;
                 r_mdm_phys_addr.COMMENTS := i.comments;
                 --r_mdm_phys_addr.ADDRESS_TYPE := i.address_type; -- JC
                 IF V_COUNT_SEQ  = 1 AND i.address_type != 'OFFICE' THEN  -- JC
                    r_mdm_phys_addr.ADDRESS_TYPE := 'OFFICE';  -- JC
                 ELSE  -- JC
                    r_mdm_phys_addr.ADDRESS_TYPE := i.address_type; -- JC
                 END IF ; -- JC
                 r_mdm_phys_addr.COUNTRY := i.country;
                 r_mdm_phys_addr.STATUS := i.status;
                 r_mdm_phys_addr.COUNTY := i.county;
                 r_mdm_phys_addr.COMPANY_NAME := i.company_name;
                 r_mdm_phys_addr.CONTACT_FIRST_NAME := i.contact_first_name;
                 r_mdm_phys_addr.CONTACT_LAST_NAME := i.contact_last_name;
                 r_mdm_phys_addr.PHY_CLINIC_FLAG := i.phy_clinic_flag;
                 r_mdm_phys_addr.MARKETING_ID := i.marketing_id;
                 r_mdm_phys_addr.ADDR_EFFECTIVE_DATE := i.addr_effective_date;

                 thot.addresses_pkg.insert_PRC(x_rowid
                                             , r_mdm_phys_addr
                                             , x_errors
                                             , x_retcode );
                  if x_retcode <> 0 then
                     null;   ---Log error
                     save_msg(1
                              ,LABEL_ERROR
                              ,'Unexpected error while inserting address information (loop)'||x_errors|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.update_phys_addresses'
                              ,NULL --NPI_ID
                              ,p_physician_id --PHYSICIAN_ID
                              ,NULL --DEA_ID
                              ,NULL --AIMI_HDR_ID
                              ,g_process_name);
                  end if;
            end if;
         --end loop; --JC
      end loop;
      END IF;
      --  CQ27497 JGonzalez Mar-11-2015 [
      save_msg(1
               ,LABEL_INFO
               ,'End update physician addresses. THOT.PHYSICIANS_CLEAN_UP_PKG.update_phys_addresses'
               ,NULL --NPI_ID
               ,p_physician_id --PHYSICIAN_ID
               ,NULL --DEA_ID
                ,NULL --AIMI_HDR_ID
               ,g_process_name);
      -- ] CQ27497 JGonzalez Mar-11-2015
   exception
   when others then
      save_msg(1
               ,LABEL_ERROR
               ,'Unexpected error while inserting address information '||x_errors|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.update_phys_addresses'
               ,NULL --NPI_ID
               ,p_physician_id --PHYSICIAN_ID
               ,NULL --DEA_ID
               ,NULL --AIMI_HDR_ID
               ,g_process_name);
   end update_phys_addresses;
-- +============================================================================================================+
-- |   Name: update_phys_phones                                                                             |
-- |   Description: This procedures updates(inserts) only addresses block      |
-- |   Author: Diego Resendi CQ27045                                                                            |
-- +============================================================================================================+
   procedure update_phys_phones( p_physician_id  number
                                 ,p_request_id   number) is

   cursor c_mdm_search_phone is
   SELECT *
     FROM THOT.mdm_physician_phones_resp
    WHERE id_request = p_request_id;

   cursor c_rxhome_phone is
   SELECT *
     FROM thot.phones
    WHERE name_id = p_physician_id
      AND name_type = 'D'
      --and phone_type not in ('AUTOFAX','FAX','OFFICE')  -- commented by CQ27497 JGonzalez Mar-10-2015
      AND addr_seq = 1; --Assuming seq#1

   r_mdm_phys_phone         THOT.phones%rowtype;
   x_rowid                  ROWID;
   x_errors                 VARCHAR2(100);
   x_retcode                NUMBER;
   v_phonerow_new           varchar2(100) := 'x';
   v_phonetype_new          VARCHAR2(100) := 'x';
   v_existing_addr_cnt      NUMBER;
   v_found                  NUMBER := 0;  --++ CQ27497 JGonzalez Mar-10-2015

   BEGIN
      -- CQ27497 JGonzalez Mar-11-2015 [
      save_msg(1
               ,LABEL_INFO
               ,'BEGIN update physician phones THOT.PHYSICIANS_CLEAN_UP_PKG.update_phys_phones'
               ,NULL --NPI_ID
               ,p_physician_id --PHYSICIAN_ID
               ,NULL --DEA_ID
	             ,NULL --AIMI_HDR_ID
               ,g_process_name);
      -- ] CQ27497 JGonzalez Mar-11-2015
      select count(1)
      into v_existing_addr_cnt
      from thot.phones
      where name_id = p_physician_id
      and name_type = 'D';

      if v_existing_addr_cnt > 0 then
         FOR i IN c_mdm_search_phone loop
            -- CQ27497 JGonzalez: logic should be to compare record i (mdm record) with all records j (rxhome records)
            -- CQ27497 JGonzalez Mar-10-2015 [
            IF i.addr_seq = 1
            THEN
              v_found := 0;
            -- ] CQ27497 JGonzalez Mar-10-2015
              FOR j IN c_rxhome_phone loop
                -- CQ27497 JGonzalez Mar-10-2015 [
                IF i.name_type      = j.name_type
                   AND i.addr_seq   = j.addr_seq
                   AND i.phonea     = j.phonea
                   AND i.phoneb     = j.phoneb
                   AND i.phonec     = j.phonec
                   AND i.phone_type = j.phone_type
                THEN
                  v_found := v_found + 1;
                END IF;
              END LOOP;
              IF v_found = 0 THEN
              -- ] CQ27497 JGonzalez Mar-10-2015
              -- Commented by CQ27497 JGonzalez Mar-10-2015 [
--               if /*i.phone_type not in ('AUTOFAX','FAX','OFFICE')   --Assuming first time phys will get FAX,AUTOFAX,OFFICE
--                  and*/
--                  i.phonea||i.phoneb||i.phonec != j.phonea||j.phoneb||j.phonec
--                  AND is_phys_phone_exist(j.phonea,j.phoneb,j.phonec,1,p_physician_id,i.PHONE_TYPE)='N'
--                  then
--                  --and (i.phonea||i.phoneb||i.phonec!= v_phonerow_new and i.PHONE_TYPE != v_phonetype_new) then
              -- ] End of commented code by CQ27497 JGonzalez Mar-10-2015
                   r_mdm_phys_phone.NAME_TYPE   := i.NAME_TYPE;
                   r_mdm_phys_phone.NAME_ID     := p_physician_id;
                   SELECT MAX(phone_seq)+1
                     INTO r_mdm_phys_phone.PHONE_SEQ
                     FROM thot.phones
                    WHERE name_id = p_physician_id
                      AND addr_seq = 1 --Assuming Seq#1
                      AND name_type = 'D';
                   r_mdm_phys_phone.PHONE_TYPE  := i.PHONE_TYPE;
                   r_mdm_phys_phone.PHONEA      := i.PHONEA;
                   r_mdm_phys_phone.PHONEB      := i.PHONEB;
                   r_mdm_phys_phone.PHONEC      := i.PHONEC;
                   r_mdm_phys_phone.ADDR_SEQ    := i.ADDR_SEQ;

                   thot.phones_pkg.insert_PRC(x_rowid
                                             , r_mdm_phys_phone
                                             , x_errors
                                             , x_retcode );

                   if x_retcode <> 0 then
                           null;   ---Log error
                           save_msg(1
                                    ,LABEL_ERROR
                                    ,'Unexpected error while inserting phones information (loop) '||x_errors|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.update_phys_phones'
                                    ,'PHONE_TYPE='||i.PHONE_TYPE
                                    ,'PHONE#='||i.PHONEA||'-'||i.PHONEC||'-'||i.PHONEC
                                    ,NULL --DEA_ID
                                    ,NULL --AIMI_HDR_ID
                                    ,g_process_name);
                   end if;
                   v_phonerow_new := i.phonea||i.phoneb||i.phonec;
                   v_phonetype_new := i.PHONE_TYPE;
              END IF; --++ CQ27497 JGonzalez Mar-10-2015
--             end if;  -- commented by CQ27497 JGonzalez Mar-10-2015
--            end loop; -- commented by CQ27497 JGonzalez Mar-10-2015
            END IF;   --++ CQ27497 JGonzalez Mar-10-2015
         end loop;
      else
         for i in c_mdm_search_phone loop
            r_mdm_phys_phone.NAME_TYPE := i.NAME_TYPE;
            r_mdm_phys_phone.name_id   := p_physician_id;
            SELECT nvl(MAX(phone_seq),0)+1
              INTO r_mdm_phys_phone.PHONE_SEQ
              FROM thot.phones
             WHERE name_id = p_physician_id
               AND addr_seq = 1 --Assuming Seq#1
               AND name_type = 'D';
            r_mdm_phys_phone.PHONE_TYPE := i.PHONE_TYPE;
            r_mdm_phys_phone.PHONEA     := i.PHONEA;
            r_mdm_phys_phone.PHONEB     := i.PHONEB;
            r_mdm_phys_phone.PHONEC     := i.PHONEC;
            r_mdm_phys_phone.ADDR_SEQ   := i.ADDR_SEQ;

            thot.phones_pkg.insert_PRC(x_rowid
                                    , r_mdm_phys_phone
                                    , x_errors
                                    , x_retcode );

            if x_retcode <> 0 then
                  null;   ---Log error
                  save_msg(1
                           ,LABEL_ERROR
                           ,'Unexpected error while inserting phones information (loop) '||x_errors|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.update_phys_phones'
                           ,'PHONE_TYPE='||i.PHONE_TYPE
                           ,'PHONE#='||i.PHONEA||'-'||i.PHONEC||'-'||i.PHONEC
                           ,NULL --DEA_ID
                           ,NULL --AIMI_HDR_ID
                           ,g_process_name);
            end if;
         end loop;
      END IF;
      -- CQ27497 JGonzalez Mar-11-2015 [
      save_msg(1
            ,LABEL_INFO
            ,'End update physician phones THOT.PHYSICIANS_CLEAN_UP_PKG.update_phys_phones'
            ,NULL --NPI_ID
            ,p_physician_id --PHYSICIAN_ID
            ,NULL --DEA_ID
            ,NULL --AIMI_HDR_ID
            ,g_process_name);
      -- ] CQ27497 JGonzalez Mar-11-2015
   exception
   when others then
   save_msg(1
            ,LABEL_ERROR
            ,'Unexpected error while inserting phones information '||x_errors|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.update_phys_phones'
            ,NULL --NPI_ID
            ,p_physician_id --PHYSICIAN_ID
            ,NULL --DEA_ID
				,NULL --AIMI_HDR_ID
            ,g_process_name);
   end update_phys_phones;
-- +============================================================================================================+
-- |   Name: update_phys_dea                                                                             |
-- |   Description: This procedures updates(inserts) only addresses block      |
-- |   Author: Diego Resendi CQ27045                                                                            |
-- +============================================================================================================+
procedure update_phys_dea( p_physician_id in number
                              ,p_phys_row in thot.mdm_physician_response%rowtype
                              ,p_update_dea    out varchar2) is


   cursor c_rxhome_phys is
   select *
   from thot.physicians
   where id = p_physician_id;

   cursor c_mdm_phys_dea(p_req_id number) is
   select *
   from thot.mdm_physician_dea_resp
   where id_request = p_req_id
   and rownum=1;

   v_rxh_phys_cur  thot.physicians%rowtype;
   v_name varchar2(500);
   r_mdm_phys_dea           thot.mdm_physician_dea_resp%rowtype;
   r_rxh_phys_dea           thot.physician_dea%rowtype;
   v_cnt_existing_dea      number := 0;
   e_existing_dea          exception;
   begin
      p_update_dea := 'N';

      open c_rxhome_phys;
      fetch c_rxhome_phys into v_rxh_phys_cur;
      close c_rxhome_phys;

      open c_mdm_phys_dea(p_phys_row.id_request);
      fetch c_mdm_phys_dea into r_mdm_phys_dea;
      close c_mdm_phys_dea;


      select count(1)
      into v_cnt_existing_dea
      from thot.physician_dea
      where dea_number = r_mdm_phys_dea.dea_number;

      if v_cnt_existing_dea > 0 then
         raise e_existing_dea;
      end if;



      v_name := p_phys_row.LAST||', '||p_phys_row.FIRST;
      if v_rxh_phys_cur.dea_id is null and r_mdm_phys_dea.dea_number is not null
         and (v_rxh_phys_cur.state = r_mdm_phys_dea.state and v_rxh_phys_cur.zip1=r_mdm_phys_dea.zip_code) then
         --update dea
         p_update_dea:='Y';
         r_rxh_phys_dea.dea_number := r_mdm_phys_dea.dea_number;
         r_rxh_phys_dea.expired_flag := r_mdm_phys_dea.expired_flag;
         R_RXH_PHYS_DEA.BUSINESS_ACTIVITY_CODE := R_MDM_PHYS_DEA.BUSINESS_ACTIVITY_CODE;
         R_RXH_PHYS_DEA.ADDRESS1 := R_MDM_PHYS_DEA.ADDRESS1; -- Added by CQ27616
         r_rxh_phys_dea.address2 := r_mdm_phys_dea.address2; -- Added by CQ27616
         r_rxh_phys_dea.schedule1 := r_mdm_phys_dea.schedule1;
         r_rxh_phys_dea.schedule2 := r_mdm_phys_dea.schedule2;
         r_rxh_phys_dea.schedule2n := r_mdm_phys_dea.schedule2n;
         r_rxh_phys_dea.schedule3 := r_mdm_phys_dea.schedule3;
         r_rxh_phys_dea.schedule3n := r_mdm_phys_dea.schedule3n;
         r_rxh_phys_dea.schedule4 := r_mdm_phys_dea.schedule4;
         r_rxh_phys_dea.schedule5 := r_mdm_phys_dea.schedule5;
         r_rxh_phys_dea.expiration_date := r_mdm_phys_dea.expiration_date;
         r_rxh_phys_dea.city := r_mdm_phys_dea.city;
         r_rxh_phys_dea.state := r_mdm_phys_dea.state;
         r_rxh_phys_dea.zip_code := r_mdm_phys_dea.zip_code;
         r_rxh_phys_dea.name := r_mdm_phys_dea.name;
         r_rxh_phys_dea.schedulel1 := r_mdm_phys_dea.schedulel1;

         insert into thot.physician_dea values r_rxh_phys_dea;

      end if;

   exception
   when e_existing_dea then
      p_update_dea:='Y';
      save_msg(1
               ,label_warning
               ,'DEA# Already exist, proceed to assign it to the physician - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.update_phys_dea'
               ,NULL --NPI_ID
               ,p_physician_id
               ,r_mdm_phys_dea.dea_number --DEA_ID
					,NULL --AIMI_HDR_ID
               ,g_process_name);
   when others then
      p_update_dea:='N';
      save_msg(1
               ,LABEL_ERROR
               ,'Unexpected error while inserting phones information '||sqlerrm|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.update_phys_dea'
               ,NULL  --NPI_ID
               ,p_physician_id --PHYSICIAN_ID
               ,NULL --DEA_ID
					,NULL --AIMI_HDR_ID
               ,g_process_name);
   end update_phys_dea;
-- +============================================================================================================+
-- |   Name: is_dea_valid_state                                                                             |
-- |   Description: This function validates whether a state is valid or no for updation (compliance)
-- |   Author: Diego Resendi CQ27045                                                                            |
-- +===========================================================================================================
   FUNCTION IS_DEA_VALID_STATE(P_PHYSICIAN_ID VARCHAR2
                              --,p_state         varchar2 --Revomed by CQ27616
                              ,P_REQUEST_ID   NUMBER      --Added by CQ27616
                              , P_DEA         VARCHAR2    --Added by CQ27616
                              ) return varchar2
   is

      v_state  number;
   begin

   -- Commented by CQ27616 [
  /* select state
   into v_state
   from thot.physician_dea
   where dea_number = (select dea_id from thot.physicians where id = p_physician_id);*/
   --] CQ27616
   
  -- Added by CQ27616 [ 
  select count(1) 
  into v_state
   from thot.physician_dea d
       , THOT.MDM_PHYSICIAN_DEA_RESP r
   where d.dea_number = p_dea
       and d.dea_number = r.dea_number
   and r.id_request = p_request_id 
   AND D.STATE= R.STATE ; 
   --] CQ27616
   
   if v_state = 0  then
      return 'N';
   else
      return  'Y';
   END IF;

   exception
   when others then
   save_msg(1
            ,LABEL_ERROR  --LABEL_INFO
            ,SQLERRM|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.is_dea_valid_state'
            ,NULL --NPI_ID
            ,p_physician_id --PHYSICIAN_ID
            ,NULL --DEA_ID
                                                                ,NULL --AIMI_HDR_ID
            ,g_process_name);
      return 'Y';
   end is_dea_valid_state;
-- +============================================================================================================+
-- |   Name: get_occur_index_by_tag_val                                                                             |
-- |   Description: Get the ocurrences by tag value contained
-- |   Author: Diego Resendi CQ27045                                                                            |
-- +===========================================================================================================
   function get_occur_index_by_tag_val(p_xml         clob
                                       ,p_tag        varchar2
                                       ,p_val        varchar2
                                       ,p_occur      number) return number
   is
      v_doc dbms_xmldom.domdocument;
      v_nodelist_parent dbms_xmldom.domnodelist;
      v_node_parent dbms_xmldom.domnode;
      v_tag_value_parent varchar2(1000):=null;
      v_parent_node_len  number:=null;
      v_occur number:=0;
      v_new_occur number:=0;
   begin
      v_doc := dbms_xmldom.newdomdocument(p_xml);
      v_occur:=thot.physicians_clean_up_pkg.get_occurrences_by_tag(p_xml,p_tag);
      v_nodelist_parent := dbms_xmldom.getelementsbytagname(v_doc, p_tag);
      for i in 0..v_occur loop
         v_node_parent := dbms_xmldom.getfirstchild(dbms_xmldom.item(v_nodelist_parent, i));
         v_tag_value_parent := DBMS_XMLDOM.GetNodeValue(v_node_parent);
         if v_tag_value_parent = p_val then
            v_new_occur := v_new_occur + 1;
            if v_new_occur = p_occur then
               return i;
            end if;
         end if;
         v_nodelist_parent := dbms_xmldom.getelementsbytagname(v_doc, p_tag);
      end loop;

      return null; --this means no occurence is found on the loop, hence return null CQ27073

   exception
   when others then
      return null;
   end get_occur_index_by_tag_val;
-- +============================================================================================================+
-- |   Name: is_phys_clean_up_on                                                                             |
-- |   Description: Returns the logic switch for the functionality
-- |   Author: Diego Resendi CQ27073                                                                            |
-- +===========================================================================================================
   function is_phys_clean_up_on return varchar2
   is
      v_switch varchar2(1);
   begin

   select attribute1
   into v_switch
   from thot.ft_lookups
   where type = 'PHYS_CLEAN_UP'
   and code        = 'LOGIC_SWITCH';

   return v_switch;

   exception
   when others then
      return 'N';
   end is_phys_clean_up_on;

-- +============================================================================================================+
-- |   Name: get_user_sec_profile                                                                             |
-- |   Description: returns the user security profile                                        |
-- |   Author: Diego Resendi CQ27096                                                                             |
-- +============================================================================================================+
   function get_user_sec_profile(p_user      varchar2, p_screen varchar2) return varchar2
   is
      v_profile   varchar2(100);
   begin

   select sec_profile
   into v_profile
   from thot.user_screens u
   where u.xuser       = p_user
   and u.screen_name = p_screen;--'phys';

   return v_profile;

   exception
   when no_data_found then
      return 'DEFAULT';
   end get_user_sec_profile;

-- +============================================================================================================+
-- |   Name: get_phys_id_by_npi                                                                             |
-- |   Description: returns RxHome Physician ID                                        |
-- |   Author: Diego Resendi CQ27096                                                                             |
-- +============================================================================================================+
   function get_phys_id_by_npi(p_npi_id      varchar2) return number
   is
      v_phys_id   number;
   begin
      select id
      into v_phys_id
      from thot.physicians
      where npi_id = p_npi_id;

      return v_phys_id;
   exception
   when others then
      save_msg(1
               ,LABEL_ERROR  --LABEL_INFO
               ,SQLERRM|| ' - ' || 'THOT.PHYSICIANS_CLEAN_UP_PKG.get_phys_id_by_npi'
               ,p_npi_id --NPI_ID
               ,NULL --PHYSICIAN_ID
               ,NULL --DEA_ID
					,NULL --AIMI_HDR_ID
               ,g_process_name);
      return null;
   end get_phys_id_by_npi;

-- +============================================================================================================+
-- |   Name: is_phys_phone_exist                                                                                |
-- |   Description: returns Y/N if a phone number exists in the physician records                               |
-- |   Author: Diego Resendi CQ27096                                                                            |
-- +============================================================================================================+
   function is_phys_phone_exist(p_phonea      varchar2
                              ,p_phoneb      varchar2
                              ,p_phonec      varchar2
                              ,p_addr_seq    number default 1
                              ,p_name_id     number
                              ,p_phone_type    varchar2) return varchar2
   is
      v_cnt number;
   begin

      select count(1)
      into v_cnt
      from thot.phones
      where name_type = 'D'
      and name_id  = p_name_id
      and addr_seq  = p_addr_seq
      and phone_type = p_phone_type
      and phonea = p_phonea
      and phoneb = p_phoneb
      and phonec = p_phonec;

      if v_cnt > 0 then
         return 'Y';
      else
         return 'N';
      end if;
   exception
      when others then
      return 'N';
   end is_phys_phone_exist;

-- +============================================================================================================+
-- |   Name: is_phys_addr_exist                                                                                 |
-- |   Description: returns Y/N if a address information exists in the physician records                        |
-- |   Author: Diego Resendi CQ27096                                                                            |
-- +============================================================================================================+
   function is_phys_addr_exist(p_addr1      varchar2
                              ,p_addr2      varchar2
                              ,p_state      varchar2
                              ,p_zip        varchar2
                              ,p_name_id    number
                              ,p_city       varchar2 ) return varchar2
   is
      v_cnt number;
   begin
      -- jcorona  adding NVL to the address validation 
      select count(1)
      into v_cnt
      from thot.addresses
      where name_type = 'D'
      and name_id  = p_name_id
      and nvl(addr1,'x') = nvl(p_addr1,'x')
      and nvl(addr2,'x') = nvl(p_addr2,'x')
      and nvl(state,'x') = nvl(p_state, 'x') 
      and nvl(zip, 'x')  = nvl(p_zip, 'x')
      and nvl(city , 'x') = nvl(p_city, 'x') ;       

      if v_cnt > 0 then
         return 'Y';
      else
         return 'N';
      end if;
   exception
      when others then
      return 'N';
   end is_phys_addr_exist;


-- +============================================================================================================+
-- |   Name: get_valid_phy_credential                                                                           |
-- |   Description: returns the actual value of physician_credentials as long as the value exists               |
-- |   Author: Diego Resendi CQ27096                                                                            |
-- +============================================================================================================+
   function get_valid_phy_credential(p_phy_credential      varchar2) return varchar2
   is
      v_phy_credentials thot.physician_credentials.phy_credential%type;
   begin

      select phy_credential
      into v_phy_credentials
      from thot.physician_credentials
      where phy_credential = p_phy_credential;

      return v_phy_credentials;

   exception
   when others then
      return null;
   end get_valid_phy_credential;


-- +============================================================================================================+
-- |   Name: validate_existing_phys_p                                                                           |
-- |   Description: Validates if physician exists by DEA and NPI present                                        |
-- |   Author: Diego Resendi CQ27163                                                                            |
-- +============================================================================================================+
   procedure validate_existing_phys_p( p_dea_id  in thot.physicians.dea_id%type
                                       ,p_npi_id in thot.physicians.npi_id%type
                                       ,p_physician_id in thot.physicians.id%type default null
                                       ,x_result_msg   out varchar2
                                       ,x_msg_type     out NUMBER)
   is
      v_phys_name   varchar2(500);
      v_phys_id     number;

      cursor c_exist_phys_prof is
      select id,first||' '||last
      into v_phys_id,v_phys_name
      from thot.physicians
      where npi_id = p_npi_id
      and dea_id = p_dea_id
      and id != nvl(p_physician_id,0)
      order by creation_date;

      cursor c_exist_phys_prof2 is
      select id,first||' '||last
      into v_phys_id,v_phys_name
      from thot.physicians
      where (npi_id = nvl(p_npi_id,'x') or dea_id = nvl(p_dea_id,'x'))
      and id != nvl(p_physician_id,0)
      order by creation_date;
   begin

      x_result_msg := null;

      if p_dea_id is not null and p_npi_id is not null then
         open c_exist_phys_prof;
         fetch c_exist_phys_prof into v_phys_id,v_phys_name;
         close c_exist_phys_prof;
         if v_phys_id is not null and v_phys_name is not null then
            x_result_msg := 'This NPI# and DEA# is already assigned to the following Physician.'|| chr(10)
                        || 'ID: ' || v_phys_id || chr(10)
                        || 'Name: ' ||	v_phys_name|| chr(10);
         -- jcorona start
            x_msg_type  := 2; -- two means hard stop
         else
            open c_exist_phys_prof2;
           fetch c_exist_phys_prof2 into v_phys_id,v_phys_name;
           close c_exist_phys_prof2;
              if v_phys_id is not null and v_phys_name is not null then
                 x_result_msg := 'This NPI# or DEA# is already assigned to the following Physician.'|| chr(10)
                        || 'ID: ' || v_phys_id || chr(10)
                        || 'Name: ' ||	v_phys_name|| chr(10)
                        || 'Do you want to proceed?';
              x_msg_type  := 1; -- 1 means soft stop
              end if;
         -- jcorona end
         end if;
      else
         open c_exist_phys_prof2;
         fetch c_exist_phys_prof2 into v_phys_id,v_phys_name;
         close c_exist_phys_prof2;
         if v_phys_id is not null and v_phys_name is not null then
            x_result_msg := 'This NPI# or DEA# is already assigned to the following Physician.'|| chr(10)
                        || 'ID: ' || v_phys_id || chr(10)
                        || 'Name: ' ||	v_phys_name|| chr(10)
                        || 'Do you want to proceed?';
            x_msg_type  := 1; -- 1 means soft stop
         end if;
      end if;

   exception
      when others then
      x_result_msg := null;
      x_msg_type   := null;
   end validate_existing_phys_p;

END PHYSICIANS_CLEAN_UP_PKG;
