create or replace PACKAGE      PHYSICIANS_CLEAN_UP_PKG AS 




        LABEL_ERROR                   CONSTANT VARCHAR2(30) := 'ERROR';
        LABEL_INFO                    CONSTANT VARCHAR2(30) := 'INFO';
        LABEL_WARNING                 CONSTANT VARCHAR2(30) := 'WARNING';
        e_inserting_physician         exception;
        e_invalid_qualifier           exception;
       


-- +---------------------------------------------------------------------------+
-- | Name: send_email                                                       |
-- | Description: Get the name for the XUSER on User's table .                 |
-- |                                                                           |
-- | Parameters:                                                               |
-- |     user = USER.XUSER                                                     |
-- | Return:                                                                   |
-- |     USER.NAME                                                             |
-- +---------------------------------------------------------------------------+ 
   procedure send_email(p_start_date DATE
                        , p_end_date DATE);
   
   
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
                              RETURN VARCHAR2;
                              
                              
-- +---------------------------------------------------------------------------+
-- | Name: send_email_physician_exception                                      |
-- | Description: Consult THOT.MDM_ENRICHMENT_EXCEPTIONS to retrieve data      |
-- |                between the parameters p_start_date and p_end_date         |
-- | Parameters:                                                               |
-- |     p_start_date                                                          |
-- |     p_end_date                                                            |
-- |                                                                           |
-- |                                                                           |
-- +---------------------------------------------------------------------------+ 
   PROCEDURE send_email_physician_exception(p_start_date DATE
                                        ,p_end_date DATE);





-- +---------------------------------------------------------------------------+
-- | Name: save_msg                                                            |
-- | Description: Insert on ft_messages the messages from this package         |
-- |                                                                           |
-- | Parameters                                                                |
-- |     p_source_id                                                           |
-- |     p_msg_type                                                            |
-- |     p_message                                                             |
-- |     p_npi_id                                                              |
-- |     p_physician_id                                                        |
-- |     p_physician_dea_id                                                    |
-- |     p_invoke_process                                                      |
-- +---------------------------------------------------------------------------+      
         PROCEDURE save_msg( p_source_id NUMBER   DEFAULT 1
                          , p_msg_type   VARCHAR2 DEFAULT 'INFO'
                          ,p_message   VARCHAR2
                          ,p_npi_id THOT.PHYSICIANS.NPI_ID%TYPE
                          ,p_physician_id THOT.PHYSICIANS.ID%TYPE
                          ,p_physician_dea_id THOT.PHYSICIANS.DEA_ID%TYPE
								  ,p_aimi_header_id RXH_CUSTOM.web_services_trans.ID%TYPE
                          , p_invoke_process VARCHAR2); 
                    
-- +============================================================================================================+
-- |   Name: parse_phys_mdm_response                                                                             |
-- |   Description: This Procedure will parse the incoming XML and  insert the data into staging tables for MDM  |
-- |   Parameters: p_response_id : Id that identifies the WS response                                            |
-- |               p_xml     : Incoming XML repsonse                                                             |
-- |   Author: Diego Resendi CQ27045                                                                             |
-- +============================================================================================================+
   procedure parse_phys_mdm_response(p_response_id number ,p_xml   clob);
   
-- +============================================================================================================+
-- |   Name: get_tag_pos_by_val                                                                                 |
-- |   Description: This function returns tag position, searching for tag name using dbms_xmldom library        |
-- |   Parameters: p_xml     : Incoming XML repsonse                                                            |
-- |               p_tag      : Tag name                                                                        |
-- |               p_search_val      : The actual value within the tag                                          |
-- |   Returns:    postition of the tag                                                                         |
-- |   Author: Diego Resendi CQ27045                                                                            |
-- +============================================================================================================+
   function get_tag_pos_by_val(p_xml         clob
                              ,p_tag         varchar2
                              ,p_search_val  varchar2 
                              ,p_pos_val     number default null) return number;

-- +============================================================================================================+
-- |   Name: get_tag_value                                                                                      |
-- |   Description: This function returns tag value, searching for tag name and position                        |
-- |   Parameters: p_xml     : Incoming XML repsonse                                                            |
-- |               p_tag      : Tag name                                                                        |
-- |               p_pos      : the actual position                                                             |
-- |   Returns:    value of the tag                                                                             |
-- |   Author: Diego Resendi CQ27045                                                                            |
-- +============================================================================================================+                              
   function get_tag_value(p_xml          clob
                           ,p_tag   varchar2
                           ,p_pos   varchar2 default null) return varchar2;

-- +============================================================================================================+
-- |   Name: get_occurrences_by_tag                                                                             |
-- |   Description: This function returns tag ocurrences, searching for tag name using dbms_xmldom library      |
-- |   Parameters: p_xml     : Incoming XML repsonse                                                            |
-- |               p_tag      : Tag name                                                                        |
-- |   Returns:    number of ocurences of the tag                                                               |
-- |   Author: Diego Resendi CQ27045                                                                            |
-- +============================================================================================================+                    
   function get_occurrences_by_tag(p_xml         clob
                                 ,p_tag         varchar2 ) return number;
                                 
-- +=============================================================================================================+
-- |   Name: parse_and_validate_found                                                                            |
-- |   Description: This Procedure will parse ans look if no physician is found upon response                    |
-- |   Parameters: p_response_id : Id that identifies the WS response                                            |
-- |   Returns: new status RESP_PHYSICIAN_NOT_FOUND or null                                                      |
-- |   Author: Diego Resendi CQ27045                                                                             |
-- +============================================================================================================+
   function parse_and_validate_found(p_response_id number) return varchar2; --CQ27045
-- +============================================================================================================+
-- |   Name: exist_physician_dea                                                                             |
-- |   Description: This functions determines if the DEA code exists already in RxHome  |
-- |   Parameters: p_dea_number
-- |   Author: Diego Resendi CQ27045                                                                             |
-- +============================================================================================================+
   FUNCTION exist_physician_dea(p_dea_number THOT.PHYSICIAN_DEA.dea_number%TYPE) RETURN NUMBER;  --CQ27045      
 -- +============================================================================================================+
-- |   Name: insert_specialty                                                                             |
-- |   Description: This function retunrs the specialty description  |
-- |   Parameters: p_code :                                             |
-- |               p_description     : |
-- |   Author: Diego Resendi CQ27045                                                                             |
-- +============================================================================================================+  
   FUNCTION insert_specialty(p_code THOT.SPECIALTIES.code%TYPE, p_description THOT.SPECIALTIES.description%TYPE) RETURN VARCHAR2; --CQ27045
-- +============================================================================================================+
-- |   Name: exist_physician                                                                                    |
-- |   Description: Determines if the NPI already exists                                                        |
-- |   Parameters: p_npi_id : NPI number                                                                        |
-- |   Author: Diego Resendi CQ27045                                                                            |
-- +============================================================================================================+   
   FUNCTION exist_physician(p_npi_id THOT.PHYSICIANS.npi_id%TYPE) RETURN NUMBER; --CQ27045
-- +============================================================================================================+
-- |   Name: send_message                                                                                       |
-- |   Description: This Procedure sends the request to MDM Service                                             |
-- |   Parameters: p_value : Identifier value                                                                   |
-- |               p_qualifier     : DEA or NPI                                                                 |
-- |   Author: Diego Resendi CQ27045                                                                            |
-- +============================================================================================================+   
   FUNCTION send_message(p_value THOT.MDM_PHYSICIAN_RESPONSE.npi_id%TYPE, p_qualifier VARCHAR2) return NUMBER; --CQ27045
-- +============================================================================================================+
-- |   Name: insert_new_physician                                                                             |
-- |   Description: This Procedure inserts new physician record and its attributes
-- |   Parameters: p_npi_id : NPI 
-- |               p_physician_id     : Physician number
-- |               p_phys_row : Physicians main row
-- |               p_request_id NUMBER
-- |   Author: Diego Resendi CQ27045                                                                             |
-- +============================================================================================================+   
   PROCEDURE insert_new_physician( p_npi_id IN VARCHAR2
                                 , p_physician_id OUT THOT.PHYSICIANS.id%TYPE
                                 , P_PHYS_ROW      NUMBER 
								                 , P_REQUEST_ID IN NUMBER --CQ27651
								                 ); --CQ27045
-- +============================================================================================================+
-- |   Name: update_physician                                                                             |
-- |   Description: This procedure updates the physician with the staging information selected on screen
-- |   Parameters: p_npi_id : Id that identifies the WS response                                            |
-- |               p_physician_id     : Incoming XML repsonse    
-- |               p_row_id         
-- |   Author: Diego Resendi CQ27045                                                                             |
-- +============================================================================================================+   
   PROCEDURE update_physician(p_npi_id             IN THOT.PHYSICIANS.npi_id%TYPE
                              ,p_physician_id      IN THOT.PHYSICIANS.id%TYPE
                              ,P_ROW_ID            IN NUMBER
							                ,p_request_id        IN NUMBER --CQ27651
                              ,x_retcode           OUT NUMBER
                              ,x_error_code        OUT VARCHAR2);--CQ27045
-- +============================================================================================================+
-- |   Name: get_occurrences_by_tag_val                                                                             |
-- |   Description: Function returns back the occurences by Tag Value
-- |   Parameters: p_response_id : Id that identifies the WS response                                            |
-- |               p_xml     : Incoming XML repsonse                                                             |
-- |   Author: Diego Resendi CQ27045                                                                             |
-- +============================================================================================================+
   function get_occurrences_by_tag_val(p_xml         clob
                                       ,p_tag        varchar2 
                                       ,p_val        varchar2) return number;--CQ27045
                                       
-- +============================================================================================================+
-- |   Name: update_phys_addresses                                                                             |
-- |   Description: Updates the addresses information accordingly
-- |   Parameters: p_response_id : Id that identifies the WS response                                            |
-- |               p_xml     : Incoming XML repsonse                                                             |
-- |   Author: Diego Resendi CQ27045                                                                             |
-- +============================================================================================================+                                       
   PROCEDURE UPDATE_PHYS_ADDRESSES( P_PHYSICIAN_ID NUMBER   
                                   ,P_REQUEST_ID   NUMBER
                                   ,P_DEA_ID       VARCHAR2 -- Added by Carlos Rasso CQ27587
                                  ); --CQ27045
-- +============================================================================================================+
-- |   Name: update_phys_phones                                                                             |
-- |   Description: Updates the addresses information accordingly
-- |   Parameters: p_response_id : Id that identifies the WS response                                            |
-- |               p_xml     : Incoming XML repsonse                                                             |
-- |   Author: Diego Resendi CQ27045                                                                             |
-- +============================================================================================================+
   procedure update_phys_phones( p_physician_id  number   
                                 ,p_request_id   number); --CQ27045
 -- +============================================================================================================+
-- |   Name: update_phys_dea                                                                             |
-- |   Description: Updates the dea information accordingly
-- |   Parameters: p_response_id : Id that identifies the WS response                                            |
-- |               p_xml     : Incoming XML repsonse                                                             |
-- |   Author: Diego Resendi CQ27045                                                                             |
-- +============================================================================================================+                                
   procedure update_phys_dea( p_physician_id in number   
                              ,p_phys_row in thot.mdm_physician_response%rowtype
                              ,p_update_dea    out varchar2); --CQ27045
-- +============================================================================================================+
-- |   Name: is_dea_valid_state                                                                             |
-- |   Description: Validates DEA's state for compliance
-- |   Parameters: p_response_id : Id that identifies the WS response                                            |
-- |               p_xml     : Incoming XML repsonse                                                             |
-- |   Author: Diego Resendi CQ27045                                                                             |
-- +============================================================================================================+
   FUNCTION IS_DEA_VALID_STATE(P_PHYSICIAN_ID  VARCHAR2
                              --,p_state         varchar2)  --CQ27045 -- Revomed by CQ27616 
                              ,P_REQUEST_ID    NUMBER    --Added by CQ27616
                              ,P_DEA          VARCHAR2) --Added by CQ27616
                               return varchar2;

-- +============================================================================================================+
-- |   Name: get_occur_index_by_tag_val                                                                             |
-- |   Description: Gets the index's occurences by tag value
-- |   Parameters: p_response_id : Id tbyhat identifies the WS response                                            |
-- |               p_xml     : Incoming XML repsonse                                                             |
-- |   Author: Diego Resendi CQ27045                                                                             |
-- +============================================================================================================+
   function get_occur_index_by_tag_val(p_xml         clob
                                       ,p_tag        varchar2
                                       ,p_val        varchar2
                                       ,p_occur      number) return number;  --CQ27045

-- +============================================================================================================+
-- |   Name: is_phys_clean_up_on                                                                             |
-- |   Description: Indicates if the logic is ON/OFF as Y/N                                                      |
-- |   Author: Diego Resendi CQ27073                                                                             |
-- +============================================================================================================+   
   function is_phys_clean_up_on return varchar2;

-- +============================================================================================================+
-- |   Name: get_user_sec_profile                                                                             |
-- |   Description: returns the user security profile                                        |
-- |   Author: Diego Resendi CQ27096                                                                             |
-- +============================================================================================================+   
   function get_user_sec_profile(p_user      varchar2,   p_screen varchar2) return varchar2;
   
-- +============================================================================================================+
-- |   Name: get_phys_id_by_npi                                                                             |
-- |   Description: returns RxHome Physician ID                                        |
-- |   Author: Diego Resendi CQ27096                                                                             |
-- +============================================================================================================+   
   function get_phys_id_by_npi(p_npi_id      varchar2) return number;
   
-- +============================================================================================================+
-- |   Name: is_phys_phone_exist                                                                                |
-- |   Description: returns Y/N if a phone number exist in the ohysician records                                |
-- |   Author: Diego Resendi CQ27096                                                                            |
-- +============================================================================================================+   
   function is_phys_phone_exist(p_phonea      varchar2
                              ,p_phoneb      varchar2
                              ,p_phonec      varchar2
                              ,p_addr_seq    number default 1
                              ,p_name_id     number
                              ,p_phone_type    varchar2) return varchar2;
   
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
                              ,p_city       varchar2) return varchar2;  -- JC 

-- +============================================================================================================+
-- |   Name: get_valid_phy_credential                                                                           |
-- |   Description: returns the actual value of physician_credentials as long as the value exists               |
-- |   Author: Diego Resendi CQ27096                                                                            |
-- +============================================================================================================+   
   function get_valid_phy_credential(p_phy_credential      varchar2) return varchar2;

-- +============================================================================================================+
-- |   Name: validate_existing_phys_p                                                                           |
-- |   Description: Validates if physician exists by DEA and NPI present                                        |
-- |   Author: Diego Resendi CQ27163                                                                            |
-- +============================================================================================================+    
   procedure validate_existing_phys_p( p_dea_id  in thot.physicians.dea_id%type
                                       ,p_npi_id in thot.physicians.npi_id%type
                                       ,p_physician_id in thot.physicians.id%type default null
                                       ,x_result_msg   out varchar2
                                       ,x_msg_type     out NUMBER);
END PHYSICIANS_CLEAN_UP_PKG;
