create or replace PACKAGE BODY      A2R_MIGRATION_PULL_PKG AS


  g_run_id        NUMBER;
  g_error_msg     rxh_stage.a2r_bulk_errors_log.error_msg@rxh_stg%type;
  g_index_err     rxh_stage.a2r_bulk_errors_log.error_index@rxh_stg%type := 0;
  g_perc          NUMBER := 0;
  g_max_perc      CONSTANT NUMBER := TO_NUMBER(rxh_stage.a2r_migration_pkg.get_migration_parameter@rxh_stg('MAX_ERROR_PERCENTAGE'));

  PROCEDURE run_migration_pull (p_thread_id NUMBER)
  IS
    v_context  rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.RUN_MIGRATION_PULL';
    v_pts_nm   NUMBER;
    v_env      VARCHAR2(100) := NULL;
  BEGIN

--===============START PULL===============
    g_run_id    := rxh_stage.a2r_migration_pkg.get_run_id@rxh_stg(p_thread_id => p_thread_id);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => '-----Main Procedure started----', p_run_id => g_run_id);
    rxh_stage.a2r_migration_pkg.insert_thread_info_pull@rxh_stg(p_run_id => g_run_id);
--=============== Users ===================
    pull_a2r_users_xref;
    pull_users;
    pull_pharmacists;
    pull_technicians;
--======================================
--===============Physicians================
    pull_npi_table;
    pull_physician_dea;
    pull_physicians;
    pull_phy_addresses;
    pull_phy_phones;
    pull_attributes;
--=====================================
--==============Patien table===============

    -- Decrypt SSN
    rxh_stage.a2r_patients_pkg.crypt_decrypt_fields@rxh_stg(p_instruction => 'D',
                                                            p_table_owner => 'RXH_STAGE',
                                                            p_table_name  => 'PATIENTS_TABLE',
                                                            p_column_name => 'SOCSEC_CRYPTED',
                                                            p_run_id      => g_run_id);
    pull_patients_table;
    pull_mi_patients_xref;  --CQ22731 New procedure for TIER 3
    -- Encrypt SSN
    rxh_stage.a2r_patients_pkg.crypt_decrypt_fields@rxh_stg(p_instruction => 'E',
                                                            p_table_owner => 'RXH_STAGE',
                                                            p_table_name  => 'PATIENTS_TABLE',
                                                            p_column_name => 'SOCSEC_CRYPTED',
                                                            p_run_id      => g_run_id);
    pull_addresses;
    pull_phones;
    -- Decrypt SSN
    rxh_stage.a2r_patients_pkg.crypt_decrypt_fields@rxh_stg(p_instruction => 'D',
                                                            p_table_owner => 'RXH_STAGE',
                                                            p_table_name  => 'PATIENT_RELATIONS',
                                                            p_column_name => 'SOCSEC_CRYPTED',
                                                            p_run_id      => g_run_id);
    pull_patient_relations;
    -- Encrypt SSN
    rxh_stage.a2r_patients_pkg.crypt_decrypt_fields@rxh_stg(p_instruction => 'E',
                                                            p_table_owner => 'RXH_STAGE',
                                                            p_table_name  => 'PATIENT_RELATIONS',
                                                            p_column_name => 'SOCSEC_CRYPTED',
                                                            p_run_id      => g_run_id);
    pull_patient_diagnoses;
    pull_patient_drug_allergies;
    pull_patient_nondrug_allergies;
    pull_patient_refsource;
    pull_patient_physician;
    pull_patient_measurement;
    pull_patient_status;
    pull_patient_therapies;
    pull_assigned_ids;

    -- Decrypt AN
    rxh_stage.a2r_patients_pkg.crypt_decrypt_fields@rxh_stg(p_instruction => 'D',
                                                            p_table_owner => 'RXH_STAGE',
                                                            p_table_name  => 'CREDIT_CARDS',
                                                            p_column_name => 'CARD_NUMBER',
                                                            p_run_id      => g_run_id);
    pull_credit_cards;
    -- Encrypt AN
    rxh_stage.a2r_patients_pkg.crypt_decrypt_fields@rxh_stg(p_instruction => 'E',
                                                            p_table_owner => 'RXH_STAGE',
                                                            p_table_name  => 'CREDIT_CARDS',
                                                            p_column_name => 'CARD_NUMBER',
                                                            p_run_id      => g_run_id);
    pull_cc_pt_details;
    pull_ms_sddb;
--======================================
--=============== User Flags================
 --======================================
    pull_user_flag_groups;
    pull_user_flag_types;
    pull_pt_user_flag_types;
--======================================
--===============Patient Insurance============
    pull_subscriber_employer;
    -- Decrypt SSN
    rxh_stage.a2r_patients_pkg.crypt_decrypt_fields@rxh_stg(p_instruction => 'D',
                                                            p_table_owner => 'RXH_STAGE',
                                                            p_table_name  => 'PATIENT_INSURANCE',
                                                            p_column_name => 'SOCSEC_CRYPTED',
                                                            p_run_id      => g_run_id);
    pull_patient_insurance;
    -- Encrypt SSN
    rxh_stage.a2r_patients_pkg.crypt_decrypt_fields@rxh_stg(p_instruction => 'E',
                                                            p_table_owner => 'RXH_STAGE',
                                                            p_table_name  => 'PATIENT_INSURANCE',
                                                            p_column_name => 'SOCSEC_CRYPTED',
                                                            p_run_id      => g_run_id);
    pull_auth;
    pull_auth_detail;
--======================================
--===============Prescription===============
    --pull_icslocs; -->> [CQ23641 16.2 RBURGOS  : Procedure was moved to stand alone procedure with new logic]
    pull_prescriptions_table;
    pull_prescription_drugs;
    pull_noncompounded_detail;
    --pull_compounding_record; -->>CQ22625
    --pull_compounding_detail; -->>CQ22625
    pull_ancillary_prescriptions;
    pull_shipments;
    pull_shipment_rxs;
    pull_shipment_items;
    pull_shipping;
    pull_shipping_dtl;
    pull_shipping_dtl_rxs;
    pull_audit_events;
    pull_rx_audit;
    pull_rx_refill_rules;
    pull_ext_sys_pkey_xref;
    pull_mi_ext_prescription_xref;  --CQ22731 New procedure for TIER 3
    complete_pull_rx;
--=======================================
    pull_charts;
    pull_med_profile;

--=======================================
--=============== Ims Documents ==============
    pull_ims_documents;
--=======================================
--=============== POM ==============
    pull_wq_tasks;
    pull_wq_tasks_history;
--=======================================
    v_pts_nm := rxh_stage.a2r_migration_pkg.get_patients_not_migrated@rxh_stg(p_run_id => g_run_id);

    BEGIN
    --
       SELECT UPPER(instance_name)
         INTO v_env
         FROM v$instance;
    --
    EXCEPTION
         WHEN OTHERS THEN
              v_env := NULL;
    END;
    rxh_stage.a2r_migration_pkg.update_thread_info_pull@rxh_stg(p_run_id      => g_run_id,
                                                                p_pts_not_mig => v_pts_nm,
                                                                p_env         => v_env
                                                               );


    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => '-----Main Procedure successfully finished----', p_run_id => g_run_id);
  END run_migration_pull;

--====================================================+
--| DISABLE_TRIGGERS: Procedure to dsiable triggers to the table                       |
--====================================================+
  PROCEDURE disable_triggers (p_table_owner rxh_stage.a2r_triggers.table_owner@rxh_stg%TYPE, p_table_name rxh_stage.a2r_triggers.table_name@rxh_stg%TYPE, t_triggers OUT triggers_tab)
  IS
    -- Cursor to obtain only the enabled triggers that we should disable
    CURSOR c_triggers
    IS
      SELECT   t.*
          FROM rxh_stage.a2r_triggers@rxh_stg t
         WHERE t.table_owner = p_table_owner
           AND t.table_name = p_table_name
           AND DISABLE = 'Y'
           AND EXISTS(SELECT 1
                        FROM SYS.all_triggers
                       WHERE owner = t.trigger_owner
                         AND trigger_name = t.trigger_name
                         AND status = 'ENABLED')
      ORDER BY trigger_owner
              ,trigger_name;
  BEGIN
    OPEN c_triggers;

    -- Fetch the triggers we will disable into the PL/SQL table
    FETCH c_triggers
    BULK COLLECT INTO t_triggers;

    CLOSE c_triggers;

    -- Disable only the enabled triggers that we should
    IF t_triggers.COUNT > 0 THEN
      FOR i IN t_triggers.FIRST .. t_triggers.LAST LOOP
        EXECUTE IMMEDIATE ('ALTER TRIGGER ' || t_triggers(i).trigger_owner || '.' || t_triggers(i).trigger_name || ' DISABLE');
      END LOOP;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      IF c_triggers%ISOPEN THEN
        CLOSE c_triggers;
      END IF;

      RAISE;
  END disable_triggers;

--====================================================+
--| ENABLE_TRIGGERS: Procedure to enable triggers to the table                         |
--====================================================+
  PROCEDURE enable_triggers (t_triggers triggers_tab)
  IS
  BEGIN
    IF t_triggers.COUNT > 0 THEN
      FOR i IN t_triggers.FIRST .. t_triggers.LAST LOOP
        EXECUTE IMMEDIATE ('ALTER TRIGGER ' || t_triggers(i).trigger_owner || '.' || t_triggers(i).trigger_name || ' ENABLE');
      END LOOP;
    END IF;
  END enable_triggers;

--================================================================+
--| COMPLETE_PULL_RX: Procedure to update shipment_id and shipping_id from prescriptions table  |
--================================================================+
  PROCEDURE complete_pull_rx
  IS
    CURSOR prescriptions_table_cur
    IS
      SELECT svcbr_id
            ,prescription_id
            ,refill_no
            ,shipment_id
            ,shipping_id
        FROM rxh_stage.a2r_pull_rx@rxh_stg rx
       WHERE rx.run_id = g_run_id
         AND EXISTS(SELECT 1
                      FROM rxh_stage.prescriptions_table@rxh_stg stg
                     WHERE stg.processed_flag = 'Y'
                       AND stg.svcbr_id = rx.svcbr_id
                       AND stg.prescription_id = rx.prescription_id
                       AND stg.refill_no = rx.refill_no
                       AND stg.run_id = g_run_id);

	CURSOR shipments_cur
    IS
      SELECT DISTINCT shipment_id, shipping_id
        FROM rxh_stage.a2r_pull_rx@rxh_stg rx
       WHERE rx.run_id = g_run_id
         AND EXISTS(SELECT 1
                      FROM rxh_stage.prescriptions_table@rxh_stg stg
                     WHERE stg.processed_flag = 'Y'
                       AND stg.svcbr_id = rx.svcbr_id
                       AND stg.prescription_id = rx.prescription_id
                       AND stg.refill_no = rx.refill_no
                       AND stg.run_id = g_run_id);

    TYPE prescriptions_table_tab IS TABLE OF prescriptions_table_cur%ROWTYPE;
	TYPE shipments_tab IS TABLE OF shipments_cur%ROWTYPE;

    t_prescriptions_table  prescriptions_table_tab;
	t_shipments            shipments_tab;
    v_context              rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.COMPLETE_PULL_RX';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);

    FOR t_prescriptions_table IN prescriptions_table_cur LOOP
        BEGIN
          UPDATE thot.prescriptions_table rx
             SET rx.shipping_id = t_prescriptions_table.shipping_id
                ,rx.shipment_id = t_prescriptions_table.shipment_id
           WHERE rx.svcbr_id = t_prescriptions_table.svcbr_id
             AND rx.prescription_id = t_prescriptions_table.prescription_id
             AND rx.refill_no = t_prescriptions_table.refill_no;

          COMMIT;
        EXCEPTION
          WHEN OTHERS THEN
            rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context       => v_context
                                                   ,p_error_code    => SQLCODE
                                                   ,p_error_msg     => 'Error when updating prescriptions: ' || SQLERRM
                                                   ,p_attr1_name    => 'SVCBR_ID'
                                                   ,p_attr1_value   => t_prescriptions_table.svcbr_id
                                                   ,p_attr2_name    => 'PRESCRIPTION_ID'
                                                   ,p_attr2_value   => t_prescriptions_table.prescription_id
                                                   ,p_attr3_name    => 'REFILL_NO'
                                                   ,p_attr3_value   => t_prescriptions_table.refill_no
                                                   ,p_run_id => g_run_id);
            ROLLBACK;
        END;
    END LOOP;

    FOR t_shipments IN shipments_cur LOOP
        BEGIN
          UPDATE thot.shipments shp
             SET shp.shipping_id = t_shipments.shipping_id
           WHERE shp.id          = t_shipments.shipment_id;

          UPDATE thot.shipment_rxs shprx
             SET shprx.shipping_id = t_shipments.shipping_id
           WHERE shprx.shipment_id = t_shipments.shipment_id;

        COMMIT;
      EXCEPTION
        WHEN OTHERS THEN
          rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context       => v_context
                                                 ,p_error_code    => SQLCODE
                                                 ,p_error_msg     => 'Error when updating shipment: ' || SQLERRM
                                                 ,p_attr1_name    => 'SHIPMENT'
                                                 ,p_attr1_value   => t_shipments.shipment_id
                                                 ,p_attr2_name    => 'SHIPPING'
                                                 ,p_attr2_value   => t_shipments.shipping_id
                                                 ,p_run_id => g_run_id);
          ROLLBACK;
      END;
    END LOOP;

    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END complete_pull_rx;

--========================== MAIN [PULLING TABLES]=======================
--==================================================+
--| PULL_PATIENTS_TABLE: Pull Patients_Table table to rxhome.                      |
--==================================================+
  PROCEDURE pull_patients_table
  IS
    CURSOR patients_table_cur
    IS
      SELECT   ID
              ,LAST
              ,FIRST
              ,mi
              ,nickname
              ,generation
              ,cmp_id
              ,svcbr_id
              ,addr1
              ,addr2
              ,city
              ,state
              ,zip1
              ,zip2
              ,county
              ,country
              ,phone_type1
              ,phone1a
              ,phone1b
              ,phone1c
              ,phone_ext1
              ,phone_type2
              ,phone2a
              ,phone2b
              ,phone2c
              ,phone_ext2
              ,phone_type3
              ,phone3a
              ,phone3b
              ,phone3c
              ,phone_ext3
              ,NULL AS socsec
              ,sex
              ,date_of_birth
              ,language_spoken
              ,religion
              ,racial_mix
              ,invoicer
              ,marketing_id
              ,pricing_id
              ,addr_label_printok
              ,mapsco
              ,delivery_instruct
              ,military_status
              ,military_branch
              ,marital_status
              ,student_status
              ,employment_status
              ,medical_hist
              ,therapy_instructions
              ,rx_instructions
              ,therapy_duration
              ,therapy_comments
              ,rx_comments
              ,primary_therapy_type
              ,nvisits_frequency
              ,iv_device
              ,iv_date
              ,pos
              ,tos
              ,employment_flag
              ,auto_accident_flag
              ,other_accident_flag
              ,accident_state
              ,pt_category
              ,smoker_flag
              ,charges
              ,pmt_deductions
              ,adj_deductions
              ,balance_due
              ,payments
              ,adjustments
              ,unpaid
              ,last_pmt_date
              ,prescriptions_count
              ,sales_marketing_status
              ,pharmacy_status
              ,nursing_status
              ,csr_status
              ,collector
              ,occurrence_date
              ,team
              ,last_dunning_date
              ,last_dunning_profile_name
              ,last_dunning_profile_seq
              ,last_dunning_unpaid
              ,dunning_profile_name
              ,dunning_flag
              ,special_instruct
              ,coll_assign_flag
              ,icr_assign_flag
              ,last_dur_check_date
              ,nvisits_count
              ,pregnant_flag
              ,created_by
              ,last_modified_by
              ,created_date
              ,last_modified_date
              ,partnership_security_label
              ,language_spoken_lookup_id
              ,contractual_flag
              ,shipper
              ,shipper_method
              ,nkda_flag
              ,profile_last_reviewed_date
              ,profile_last_reviewed_user
              ,profile_last_comments
              ,ppay_contact_name
              ,ppay_contact_phonea
              ,ppay_contact_phoneb
              ,ppay_contact_phonec
              ,ppay_contact_ph_ext
              ,print_ppay_statement
              ,ppay_monthly_cycle
              ,ppay_quarterly_cycle
              ,fep_client_flag
              ,medco_agn
              ,primary_trc_code
              ,trc_description
              ,socsec_crypted
              ,adlr_flag
              ,escalation_flag
              ,renewal_autofax
              ,contact_flag
              ,patient_type
              ,ppay_daily_reason_code
              ,ppay_monthly_reason_code
              ,ppay_quarterly_reason_code
              ,update_history_daily
              ,update_history_monthly
              ,update_history_quarterly
              ,ppay_daily_cycle
              ,print_ppay_statement_daily
              ,print_ppay_statement_quarterly
              ,no_kdrug_allergies_flag
          FROM rxh_stage.patients_table@rxh_stg pts
         WHERE pts.processed_flag = 'N'
           AND pts.ready_flag = 'Y'
           AND pts.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.patients_table rxh
                           WHERE rxh.ID = pts.ID)
      ORDER BY ID;

    CURSOR patients_table_rowid_cur
    IS
      SELECT ID
        FROM rxh_stage.patients_table@rxh_stg pts
       WHERE pts.processed_flag = 'N'
         AND pts.ready_flag = 'Y'
         AND pts.run_id = g_run_id
         AND NOT EXISTS(SELECT 1
                          FROM thot.patients_table rxh
                         WHERE rxh.ID = pts.ID)
      ORDER BY ID;

    TYPE patients_table_tab IS TABLE OF patients_table_cur%ROWTYPE;

    TYPE patients_table_rowid_tab IS TABLE OF patients_table_rowid_cur%ROWTYPE;

    t_patients_table        patients_table_tab;
    t_patients_table_rowid  patients_table_rowid_tab;
    t_triggers              triggers_tab;
    v_context               rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PATIENTS_TABLE';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PATIENTS_TABLE', t_triggers => t_triggers);

    OPEN patients_table_cur;

    OPEN patients_table_rowid_cur;

    LOOP
      FETCH patients_table_cur
      BULK COLLECT INTO t_patients_table LIMIT bulk_limit;

      EXIT WHEN t_patients_table.COUNT = 0;

      FETCH patients_table_rowid_cur
      BULK COLLECT INTO t_patients_table_rowid LIMIT bulk_limit;

      BEGIN
        FORALL h IN 1 .. t_patients_table.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT ID
                             ,LAST
                             ,FIRST
                             ,mi
                             ,nickname
                             ,generation
                             ,cmp_id
                             ,svcbr_id
                             ,addr1
                             ,addr2
                             ,city
                             ,state
                             ,zip1
                             ,zip2
                             ,county
                             ,country
                             ,phone_type1
                             ,phone1a
                             ,phone1b
                             ,phone1c
                             ,phone_ext1
                             ,phone_type2
                             ,phone2a
                             ,phone2b
                             ,phone2c
                             ,phone_ext2
                             ,phone_type3
                             ,phone3a
                             ,phone3b
                             ,phone3c
                             ,phone_ext3
                             ,socsec
                             ,sex
                             ,date_of_birth
                             ,language_spoken
                             ,religion
                             ,racial_mix
                             ,invoicer
                             ,marketing_id
                             ,pricing_id
                             ,addr_label_printok
                             ,mapsco
                             ,delivery_instruct
                             ,military_status
                             ,military_branch
                             ,marital_status
                             ,student_status
                             ,employment_status
                             ,medical_hist
                             ,therapy_instructions
                             ,rx_instructions
                             ,therapy_duration
                             ,therapy_comments
                             ,rx_comments
                             ,primary_therapy_type
                             ,nvisits_frequency
                             ,iv_device
                             ,iv_date
                             ,pos
                             ,tos
                             ,employment_flag
                             ,auto_accident_flag
                             ,other_accident_flag
                             ,accident_state
                             ,pt_category
                             ,smoker_flag
                             ,charges
                             ,pmt_deductions
                             ,adj_deductions
                             ,balance_due
                             ,payments
                             ,adjustments
                             ,unpaid
                             ,last_pmt_date
                             ,prescriptions_count
                             ,sales_marketing_status
                             ,pharmacy_status
                             ,nursing_status
                             ,csr_status
                             ,collector
                             ,occurrence_date
                             ,team
                             ,last_dunning_date
                             ,last_dunning_profile_name
                             ,last_dunning_profile_seq
                             ,last_dunning_unpaid
                             ,dunning_profile_name
                             ,dunning_flag
                             ,special_instruct
                             ,coll_assign_flag
                             ,icr_assign_flag
                             ,last_dur_check_date
                             ,nvisits_count
                             ,pregnant_flag
                             ,created_by
                             ,last_modified_by
                             ,created_date
                             ,last_modified_date
                             ,partnership_security_label
                             ,language_spoken_lookup_id
                             ,contractual_flag
                             ,shipper
                             ,shipper_method
                             ,nkda_flag
                             ,profile_last_reviewed_date
                             ,profile_last_reviewed_user
                             ,profile_last_comments
                             ,ppay_contact_name
                             ,ppay_contact_phonea
                             ,ppay_contact_phoneb
                             ,ppay_contact_phonec
                             ,ppay_contact_ph_ext
                             ,print_ppay_statement
                             ,ppay_monthly_cycle
                             ,ppay_quarterly_cycle
                             ,fep_client_flag
                             ,medco_agn
                             ,primary_trc_code
                             ,trc_description
                             ,socsec_crypted
                             ,adlr_flag
                             ,escalation_flag
                             ,renewal_autofax
                             ,contact_flag
                             ,patient_type
                             ,ppay_daily_reason_code
                             ,ppay_monthly_reason_code
                             ,ppay_quarterly_reason_code
                             ,update_history_daily
                             ,update_history_monthly
                             ,update_history_quarterly
                             ,ppay_daily_cycle
                             ,print_ppay_statement_daily
                             ,print_ppay_statement_quarterly
                             ,no_kdrug_allergies_flag
                         FROM thot.patients_table)
               VALUES t_patients_table(h);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PATIENTS_TABLE'
                                                             ,p_run_id     => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_patients_table.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                 INSERT INTO (SELECT ID
                             ,LAST
                             ,FIRST
                             ,mi
                             ,nickname
                             ,generation
                             ,cmp_id
                             ,svcbr_id
                             ,addr1
                             ,addr2
                             ,city
                             ,state
                             ,zip1
                             ,zip2
                             ,county
                             ,country
                             ,phone_type1
                             ,phone1a
                             ,phone1b
                             ,phone1c
                             ,phone_ext1
                             ,phone_type2
                             ,phone2a
                             ,phone2b
                             ,phone2c
                             ,phone_ext2
                             ,phone_type3
                             ,phone3a
                             ,phone3b
                             ,phone3c
                             ,phone_ext3
                             ,socsec
                             ,sex
                             ,date_of_birth
                             ,language_spoken
                             ,religion
                             ,racial_mix
                             ,invoicer
                             ,marketing_id
                             ,pricing_id
                             ,addr_label_printok
                             ,mapsco
                             ,delivery_instruct
                             ,military_status
                             ,military_branch
                             ,marital_status
                             ,student_status
                             ,employment_status
                             ,medical_hist
                             ,therapy_instructions
                             ,rx_instructions
                             ,therapy_duration
                             ,therapy_comments
                             ,rx_comments
                             ,primary_therapy_type
                             ,nvisits_frequency
                             ,iv_device
                             ,iv_date
                             ,pos
                             ,tos
                             ,employment_flag
                             ,auto_accident_flag
                             ,other_accident_flag
                             ,accident_state
                             ,pt_category
                             ,smoker_flag
                             ,charges
                             ,pmt_deductions
                             ,adj_deductions
                             ,balance_due
                             ,payments
                             ,adjustments
                             ,unpaid
                             ,last_pmt_date
                             ,prescriptions_count
                             ,sales_marketing_status
                             ,pharmacy_status
                             ,nursing_status
                             ,csr_status
                             ,collector
                             ,occurrence_date
                             ,team
                             ,last_dunning_date
                             ,last_dunning_profile_name
                             ,last_dunning_profile_seq
                             ,last_dunning_unpaid
                             ,dunning_profile_name
                             ,dunning_flag
                             ,special_instruct
                             ,coll_assign_flag
                             ,icr_assign_flag
                             ,last_dur_check_date
                             ,nvisits_count
                             ,pregnant_flag
                             ,created_by
                             ,last_modified_by
                             ,created_date
                             ,last_modified_date
                             ,partnership_security_label
                             ,language_spoken_lookup_id
                             ,contractual_flag
                             ,shipper
                             ,shipper_method
                             ,nkda_flag
                             ,profile_last_reviewed_date
                             ,profile_last_reviewed_user
                             ,profile_last_comments
                             ,ppay_contact_name
                             ,ppay_contact_phonea
                             ,ppay_contact_phoneb
                             ,ppay_contact_phonec
                             ,ppay_contact_ph_ext
                             ,print_ppay_statement
                             ,ppay_monthly_cycle
                             ,ppay_quarterly_cycle
                             ,fep_client_flag
                             ,medco_agn
                             ,primary_trc_code
                             ,trc_description
                             ,socsec_crypted
                             ,adlr_flag
                             ,escalation_flag
                             ,renewal_autofax
                             ,contact_flag
                             ,patient_type
                             ,ppay_daily_reason_code
                             ,ppay_monthly_reason_code
                             ,ppay_quarterly_reason_code
                             ,update_history_daily
                             ,update_history_monthly
                             ,update_history_quarterly
                             ,ppay_daily_cycle
                             ,print_ppay_statement_daily
                             ,print_ppay_statement_quarterly
                             ,no_kdrug_allergies_flag
                         FROM thot.patients_table)
               VALUES t_patients_table(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;



              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg  --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_patients_table(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).ID
                                                               ,p_run_id     => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_patients_table_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context => v_context,
                                              p_error_msg => 'Records processed :' || t_patients_table.COUNT || ' | Records inserted:' ||(t_patients_table.COUNT - SQL%BULK_EXCEPTIONS.COUNT), p_run_id => g_run_id);

      FOR i IN 1 .. t_patients_table.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.patients_table@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE id = t_patients_table_rowid(i).id;
        EXCEPTION
          WHEN OTHERS THEN
            rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context => v_context, p_error_code => SQLCODE,
                                                    p_error_msg => SUBSTR('Error when updating flag:' || SQLERRM, 1, 4000), p_run_id => g_run_id);
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE patients_table_cur;

    CLOSE patients_table_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF patients_table_cur%ISOPEN THEN
        CLOSE patients_table_cur;
      END IF;

      IF patients_table_rowid_cur%ISOPEN THEN
        CLOSE patients_table_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_patients_table;

--=============================================+
--| PULL_ADDRESSES: Pulls ADDRESSES table to rxhome.                   |
--=============================================+
  PROCEDURE pull_addresses
  IS
    CURSOR addresses_cur
    IS
      SELECT   name_type
              ,name_id
              ,addr_seq
              ,attn
              ,NAME
              ,addr1
              ,addr2
              ,city
              ,state
              ,zip
              ,comments
              ,address_type
              ,country
              ,status
              ,county
              ,partnership_security_label
              ,company_name
              ,contact_first_name
              ,contact_last_name
              ,phy_clinic_flag
              ,marketing_id
              ,addr_effective_date
              ,created_by
              ,creation_date
              ,updated_by
              ,updated_date
              ,scc_address_flag
              ,code1_status
              ,code1_value
          FROM rxh_stage.addresses@rxh_stg atg
         WHERE name_type = 'P'
           AND processed_flag = 'N'
           AND atg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = atg.name_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.addresses rxh
                           WHERE rxh.name_type = atg.name_type
                             AND rxh.name_id = atg.name_id
                             AND rxh.addr_seq = atg.addr_seq)
      ORDER BY ROWID;

    CURSOR addresses_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.addresses@rxh_stg atg
         WHERE name_type = 'P'
           AND processed_flag = 'N'
           AND atg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = atg.name_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.addresses rxh
                           WHERE rxh.name_type = atg.name_type
                             AND rxh.name_id = atg.name_id
                             AND rxh.addr_seq = atg.addr_seq)
      ORDER BY ROWID;

    TYPE addresses_tab IS TABLE OF addresses_cur%ROWTYPE;

    TYPE addresses_rowid_tab IS TABLE OF ROWID;

    t_addresses        addresses_tab;
    t_addresses_rowid  addresses_rowid_tab;
    t_triggers         triggers_tab;
    v_context          rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_ADDRESSES';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'ADDRESSES', t_triggers => t_triggers);

    OPEN addresses_cur;

    OPEN addresses_rowid_cur;

    LOOP
      FETCH addresses_cur
      BULK COLLECT INTO t_addresses LIMIT bulk_limit;

      EXIT WHEN t_addresses.COUNT = 0;

      FETCH addresses_rowid_cur
      BULK COLLECT INTO t_addresses_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_addresses.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT name_type
                             ,name_id
                             ,addr_seq
                             ,attn
                             ,NAME
                             ,addr1
                             ,addr2
                             ,city
                             ,state
                             ,zip
                             ,comments
                             ,address_type
                             ,country
                             ,status
                             ,county
                             ,partnership_security_label
                             ,company_name
                             ,contact_first_name
                             ,contact_last_name
                             ,phy_clinic_flag
                             ,marketing_id
                             ,addr_effective_date
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,updated_date
                             ,scc_address_flag
                             ,code1_status
                             ,code1_value
                         FROM thot.addresses)
               VALUES t_addresses(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.ADDRESSES'
                                                             ,p_run_id     => g_run_id);
            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@RXH_STG(t_addresses.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                 INSERT INTO (SELECT name_type
                             ,name_id
                             ,addr_seq
                             ,attn
                             ,NAME
                             ,addr1
                             ,addr2
                             ,city
                             ,state
                             ,zip
                             ,comments
                             ,address_type
                             ,country
                             ,status
                             ,county
                             ,partnership_security_label
                             ,company_name
                             ,contact_first_name
                             ,contact_last_name
                             ,phy_clinic_flag
                             ,marketing_id
                             ,addr_effective_date
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,updated_date
                             ,scc_address_flag
                             ,code1_status
                             ,code1_value
                         FROM thot.addresses)
               VALUES t_addresses(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
                END;
              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg  --QLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_addresses(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).name_id
                                                               ,p_run_id     => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_addresses_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context => v_context,
                                              p_error_msg => 'Records processed :' || t_addresses.COUNT || ' | Records inserted:' ||(t_addresses.COUNT - SQL%BULK_EXCEPTIONS.COUNT), p_run_id => g_run_id);

      FOR i IN 1 .. t_addresses.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.addresses@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_addresses_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE addresses_cur;

    CLOSE addresses_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF addresses_cur%ISOPEN THEN
        CLOSE addresses_cur;
      END IF;

      IF addresses_rowid_cur%ISOPEN THEN
        CLOSE addresses_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_addresses;

--=======================================+
--| PULL_PHONES: Pulls PHONES table to rxhome.                  |
--=======================================+
  PROCEDURE pull_phones
  IS
    CURSOR phones_cur
    IS
      SELECT   name_type
              ,name_id
              ,phone_seq
              ,phone_type
              ,phonea
              ,phoneb
              ,phonec
              ,phone_ext
              ,phone_comment
              ,addr_seq
              ,partnership_security_label
              ,contact_flag
              ,created_by
              ,creation_date
              ,updated_by
              ,updated_date
              ,call_day
              ,call_time
              ,call_time_zone
          FROM rxh_stage.phones@rxh_stg ptg
         WHERE name_type = 'P'
           AND processed_flag = 'N'
           AND ptg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = ptg.name_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.phones rxh
                           WHERE rxh.name_type = ptg.name_type
                             AND rxh.name_id = ptg.name_id
                             AND rxh.addr_seq = ptg.addr_seq
                             AND rxh.phone_seq = ptg.phone_seq)
      ORDER BY ROWID;

    CURSOR phones_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.phones@rxh_stg ptg
         WHERE name_type = 'P'
           AND processed_flag = 'N'
           AND ptg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = ptg.name_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.phones rxh
                           WHERE rxh.name_type = ptg.name_type
                             AND rxh.name_id = ptg.name_id
                             AND rxh.addr_seq = ptg.addr_seq
                             AND rxh.phone_seq = ptg.phone_seq)
      ORDER BY ROWID;

    TYPE phones_tab IS TABLE OF phones_cur%ROWTYPE;

    TYPE phones_rowid_tab IS TABLE OF ROWID;

    t_phones        phones_tab;
    t_phones_rowid  phones_rowid_tab;
    t_triggers      triggers_tab;
    v_context       rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PHONES';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PHONES', t_triggers => t_triggers);

    OPEN phones_cur;

    OPEN phones_rowid_cur;

    LOOP
      FETCH phones_cur
      BULK COLLECT INTO t_phones LIMIT bulk_limit;

      EXIT WHEN t_phones.COUNT = 0;

      FETCH phones_rowid_cur
      BULK COLLECT INTO t_phones_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_phones.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT name_type
                             ,name_id
                             ,phone_seq
                             ,phone_type
                             ,phonea
                             ,phoneb
                             ,phonec
                             ,phone_ext
                             ,phone_comment
                             ,addr_seq
                             ,partnership_security_label
                             ,contact_flag
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,updated_date
                             ,call_day
                             ,call_time
                             ,call_time_zone
                         FROM thot.phones)
               VALUES t_phones(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PHONES'
                                                             ,p_run_id     => g_run_id);
            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_phones.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                 INSERT INTO (SELECT name_type
                             ,name_id
                             ,phone_seq
                             ,phone_type
                             ,phonea
                             ,phoneb
                             ,phonec
                             ,phone_ext
                             ,phone_comment
                             ,addr_seq
                             ,partnership_security_label
                             ,contact_flag
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,updated_date
                             ,call_day
                             ,call_time
                             ,call_time_zone
                         FROM thot.phones)
               VALUES t_phones(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_phones(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).name_id
                                                               ,p_run_id     => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_phones_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
               -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_phones.COUNT || ' | Records inserted:' ||(t_phones.COUNT - SQL%BULK_EXCEPTIONS.COUNT), p_run_id => g_run_id);

      FOR i IN 1 .. t_phones.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.phones@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_phones_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE phones_cur;

    CLOSE phones_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF phones_cur%ISOPEN THEN
        CLOSE phones_cur;
      END IF;

      IF phones_rowid_cur%ISOPEN THEN
        CLOSE phones_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_phones;


--================================================+
--| PULL_PATIENT_RELATIONS: Pulls patient_relations table to rxhome.       |
--================================================+
  PROCEDURE pull_patient_relations
  IS
    CURSOR patient_relations_cur
    IS
      SELECT   patient_id
              ,relationship
              ,FIRST
              ,mi
              ,LAST
              ,generation
              ,addr1
              ,addr2
              ,city
              ,state
              ,zip1
              ,zip2
              ,country
              ,NULL AS socsec
              ,phone_type1
              ,phone1a
              ,phone1b
              ,phone1c
              ,phone_ext1
              ,phone_type2
              ,phone2a
              ,phone2b
              ,phone2c
              ,phone_ext2
              ,responsible_party_flag
              ,emergency_contact_flag
              ,employer_name
              ,employer_addr1
              ,employer_addr2
              ,employer_city
              ,employer_state
              ,employer_zip1
              ,employer_zip2
              ,employment_status
              ,financially_responsible_flag
              ,partnership_security_label
              ,employer_country
              ,socsec_crypted
              ,created_by
              ,creation_date
              ,updated_by
              ,update_date
          FROM rxh_stage.patient_relations@rxh_stg prg
         WHERE prg.processed_flag = 'N'
           AND prg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = prg.patient_id)
      ORDER BY ROWID;

    CURSOR patient_relations_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.patient_relations@rxh_stg prg
         WHERE prg.processed_flag = 'N'
           AND prg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = prg.patient_id)
      ORDER BY ROWID;

    TYPE patient_relations_tab IS TABLE OF patient_relations_cur%ROWTYPE;

    TYPE patient_relations_rowid_tab IS TABLE OF ROWID;

    t_patient_relations        patient_relations_tab;
    t_patient_relations_rowid  patient_relations_rowid_tab;
    t_triggers                 triggers_tab;
    v_context                  rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PATIENT_RELATIONS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PATIENT_RELATIONS', t_triggers => t_triggers);

    OPEN patient_relations_cur;

    OPEN patient_relations_rowid_cur;

    LOOP
      FETCH patient_relations_cur
      BULK COLLECT INTO t_patient_relations LIMIT bulk_limit;

      EXIT WHEN t_patient_relations.COUNT = 0;

      FETCH patient_relations_rowid_cur
      BULK COLLECT INTO t_patient_relations_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_patient_relations.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT patient_id
                             ,relationship
                             ,FIRST
                             ,mi
                             ,LAST
                             ,generation
                             ,addr1
                             ,addr2
                             ,city
                             ,state
                             ,zip1
                             ,zip2
                             ,country
                             ,socsec
                             ,phone_type1
                             ,phone1a
                             ,phone1b
                             ,phone1c
                             ,phone_ext1
                             ,phone_type2
                             ,phone2a
                             ,phone2b
                             ,phone2c
                             ,phone_ext2
                             ,responsible_party_flag
                             ,emergency_contact_flag
                             ,employer_name
                             ,employer_addr1
                             ,employer_addr2
                             ,employer_city
                             ,employer_state
                             ,employer_zip1
                             ,employer_zip2
                             ,employment_status
                             ,financially_responsible_flag
                             ,partnership_security_label
                             ,employer_country
                             ,socsec_crypted
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,update_date
                         FROM thot.patient_relations)
               VALUES t_patient_relations(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PATIENT_RELATIONS'
                                                             ,p_run_id     => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_patient_relations.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                 INSERT INTO (SELECT patient_id
                             ,relationship
                             ,FIRST
                             ,mi
                             ,LAST
                             ,generation
                             ,addr1
                             ,addr2
                             ,city
                             ,state
                             ,zip1
                             ,zip2
                             ,country
                             ,socsec
                             ,phone_type1
                             ,phone1a
                             ,phone1b
                             ,phone1c
                             ,phone_ext1
                             ,phone_type2
                             ,phone2a
                             ,phone2b
                             ,phone2c
                             ,phone_ext2
                             ,responsible_party_flag
                             ,emergency_contact_flag
                             ,employer_name
                             ,employer_addr1
                             ,employer_addr2
                             ,employer_city
                             ,employer_state
                             ,employer_zip1
                             ,employer_zip2
                             ,employment_status
                             ,financially_responsible_flag
                             ,partnership_security_label
                             ,employer_country
                             ,socsec_crypted
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,update_date
                         FROM thot.patient_relations)
               VALUES t_patient_relations(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;
              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_patient_relations(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               ,p_run_id     => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_patient_relations_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_patient_relations.COUNT || ' | Records inserted:' ||(t_patient_relations.COUNT - SQL%BULK_EXCEPTIONS.COUNT), p_run_id => g_run_id);

      FOR i IN 1 .. t_patient_relations.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.patient_relations@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_patient_relations_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE patient_relations_cur;

    CLOSE patient_relations_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF patient_relations_cur%ISOPEN THEN
        CLOSE patient_relations_cur;
      END IF;

      IF patient_relations_rowid_cur%ISOPEN THEN
        CLOSE patient_relations_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_patient_relations;

  --====================================================
--| PULL_PATIENT_DIAGNOSES: Pulls PATIENT_DIAGNOSES table to rxhome.        |
--=====================================================
  PROCEDURE pull_patient_diagnoses
  IS
    CURSOR patient_diagnoses_cur
    IS
      SELECT   patient_id
              ,diag_seq
              ,icd9_code
              ,start_date
              ,stop_date
              ,xdate
              ,xuser
              ,partnership_security_label
              ,created_by
              ,creation_date
              ,updated_by
              ,update_date
          FROM rxh_stage.patient_diagnoses@rxh_stg pdt
         WHERE pdt.processed_flag = 'N'
           AND pdt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = pdt.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_diagnoses rxh
                           WHERE rxh.patient_id = pdt.patient_id
                             AND rxh.icd9_code = pdt.icd9_code)
      ORDER BY ROWID;

    CURSOR patient_diagnoses_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.patient_diagnoses@rxh_stg pdt
         WHERE pdt.processed_flag = 'N'
           AND pdt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = pdt.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_diagnoses rxh
                           WHERE rxh.patient_id = pdt.patient_id
                             AND rxh.icd9_code = pdt.icd9_code)
      ORDER BY ROWID;

    TYPE patient_diagnoses_tab IS TABLE OF patient_diagnoses_cur%ROWTYPE;

    TYPE patient_diagnoses_rowid_tab IS TABLE OF ROWID;

    t_patient_diagnoses        patient_diagnoses_tab;
    t_patient_diagnoses_rowid  patient_diagnoses_rowid_tab;
    t_triggers                 triggers_tab;
    v_context                  rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PATIENT_DIAGNOSES';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PATIENT_DIAGNOSES', t_triggers => t_triggers);

    OPEN patient_diagnoses_cur;

    OPEN patient_diagnoses_rowid_cur;

    LOOP
      FETCH patient_diagnoses_cur
      BULK COLLECT INTO t_patient_diagnoses LIMIT bulk_limit;

      EXIT WHEN t_patient_diagnoses.COUNT = 0;

      FETCH patient_diagnoses_rowid_cur
      BULK COLLECT INTO t_patient_diagnoses_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_patient_diagnoses.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT patient_id
                             ,diag_seq
                             ,icd9_code
                             ,start_date
                             ,stop_date
                             ,xdate
                             ,xuser
                             ,partnership_security_label
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,update_date
                         FROM thot.patient_diagnoses)
               VALUES t_patient_diagnoses(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PATIENT_DIAGNOSES'
                                                             ,p_run_id     => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_patient_diagnoses.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                INSERT INTO (SELECT patient_id
                             ,diag_seq
                             ,icd9_code
                             ,start_date
                             ,stop_date
                             ,xdate
                             ,xuser
                             ,partnership_security_label
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,update_date
                         FROM thot.patient_diagnoses)
               VALUES t_patient_diagnoses(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;
              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg  --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_patient_diagnoses(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               ,p_attr2_name    => 'ICD9_CODE'
                                                               ,p_attr2_value   => t_patient_diagnoses(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).icd9_code
                                                               ,p_run_id     => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_patient_diagnoses_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_patient_diagnoses.COUNT || ' | Records inserted:' ||(t_patient_diagnoses.COUNT - SQL%BULK_EXCEPTIONS.COUNT), p_run_id => g_run_id);

      FOR i IN 1 .. t_patient_diagnoses.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.patient_diagnoses@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_patient_diagnoses_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE patient_diagnoses_cur;

    CLOSE patient_diagnoses_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF patient_diagnoses_cur%ISOPEN THEN
        CLOSE patient_diagnoses_cur;
      END IF;

      IF patient_diagnoses_rowid_cur%ISOPEN THEN
        CLOSE patient_diagnoses_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_patient_diagnoses;

--=============================================================
--| PULL_PATIENT_DRUG_ALLERGIES: Pulls PATIENT_DRUG_ALLERGIES table to rxhome. |
--=============================================================
  PROCEDURE pull_patient_drug_allergies
  IS
    CURSOR patient_drug_allergies_cur
    IS
      SELECT   patient_id
              ,drug_abbrev
              ,reaction_onset_date
              ,skin_symptom
              ,shock_symptom
              ,asthma_symptom
              ,nausea_symptom
              ,anemia_symptom
              ,other_symptom
              ,other_symptom_description
              ,abbrev_type
              ,xdate
              ,xuser
              ,partnership_security_label
              ,stop_date
              ,created_by
              ,creation_date
              ,updated_by
              ,update_date
          FROM rxh_stage.patient_drug_allergies@rxh_stg pdt
         WHERE pdt.processed_flag = 'N'
           AND pdt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = pdt.patient_id)
      ORDER BY ROWID;

    CURSOR pt_drug_allergies_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.patient_drug_allergies@rxh_stg pdt
         WHERE pdt.processed_flag = 'N'
           AND pdt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = pdt.patient_id)
      ORDER BY ROWID;

    TYPE patient_drug_allergies_tab IS TABLE OF patient_drug_allergies_cur%ROWTYPE;

    TYPE pt_drug_allergies_rowid_tab IS TABLE OF ROWID;

    t_patient_drug_allergies        patient_drug_allergies_tab;
    t_patient_drug_allergies_rowid  pt_drug_allergies_rowid_tab;
    t_triggers                      triggers_tab;
    v_context                       rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PATIENT_DRUG_ALLERGIES';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PATIENT_DRUG_ALLERGIES', t_triggers => t_triggers);

    OPEN patient_drug_allergies_cur;

    OPEN pt_drug_allergies_rowid_cur;

    LOOP
      FETCH patient_drug_allergies_cur
      BULK COLLECT INTO t_patient_drug_allergies LIMIT bulk_limit;

      EXIT WHEN t_patient_drug_allergies.COUNT = 0;

      FETCH pt_drug_allergies_rowid_cur
      BULK COLLECT INTO t_patient_drug_allergies_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_patient_drug_allergies.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT patient_id
                             ,drug_abbrev
                             ,reaction_onset_date
                             ,skin_symptom
                             ,shock_symptom
                             ,asthma_symptom
                             ,nausea_symptom
                             ,anemia_symptom
                             ,other_symptom
                             ,other_symptom_description
                             ,abbrev_type
                             ,xdate
                             ,xuser
                             ,partnership_security_label
                             ,stop_date
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,update_date
                         FROM thot.patient_drug_allergies)
               VALUES t_patient_drug_allergies(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PATIENT_DRUG_ALLERGIES'
                                                             ,p_run_id     => g_run_id);
            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_patient_drug_allergies.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                INSERT INTO (SELECT patient_id
                             ,drug_abbrev
                             ,reaction_onset_date
                             ,skin_symptom
                             ,shock_symptom
                             ,asthma_symptom
                             ,nausea_symptom
                             ,anemia_symptom
                             ,other_symptom
                             ,other_symptom_description
                             ,abbrev_type
                             ,xdate
                             ,xuser
                             ,partnership_security_label
                             ,stop_date
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,update_date
                         FROM thot.patient_drug_allergies)
               VALUES t_patient_drug_allergies(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;
              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_patient_drug_allergies(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_patient_drug_allergies_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_patient_drug_allergies.COUNT || ' | Records inserted:' ||(t_patient_drug_allergies.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_patient_drug_allergies.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.patient_drug_allergies@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_patient_drug_allergies_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE patient_drug_allergies_cur;

    CLOSE pt_drug_allergies_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF patient_drug_allergies_cur%ISOPEN THEN
        CLOSE patient_drug_allergies_cur;
      END IF;

      IF pt_drug_allergies_rowid_cur%ISOPEN THEN
        CLOSE pt_drug_allergies_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_patient_drug_allergies;

--===================================================================
--| PULL_PATIENT_NONDRUG_ALLERGIES: Pulls PATIENT_NONDRUG_ALLERGIES table to rxhome. |
--===================================================================
  PROCEDURE pull_patient_nondrug_allergies
  IS
    CURSOR pt_nondrug_allergies_cur
    IS
      SELECT   patient_id
              ,allergy
              ,partnership_security_label
              ,start_date
              ,stop_date
              ,xuser
              ,xdate
              ,comments
              ,created_by
              ,creation_date
              ,updated_by
              ,update_date
          FROM rxh_stage.patient_nondrug_allergies@rxh_stg pnt
         WHERE pnt.processed_flag = 'N'
           AND pnt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = pnt.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_nondrug_allergies rxh
                           WHERE rxh.patient_id = pnt.patient_id
                             AND rxh.allergy = pnt.allergy)
      ORDER BY ROWID;

    CURSOR pt_nondrug_allergies_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.patient_nondrug_allergies@rxh_stg pnt
         WHERE pnt.processed_flag = 'N'
           AND pnt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = pnt.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_nondrug_allergies rxh
                           WHERE rxh.patient_id = pnt.patient_id
                             AND rxh.allergy = pnt.allergy)
      ORDER BY ROWID;

    TYPE pt_nondrug_allergies_tab IS TABLE OF pt_nondrug_allergies_cur%ROWTYPE;

    TYPE pt_nondrug_allergies_rowid_tab IS TABLE OF ROWID;

    t_pt_nondrug_allergies        pt_nondrug_allergies_tab;
    t_pt_nondrug_allergies_rowid  pt_nondrug_allergies_rowid_tab;
    t_triggers                    triggers_tab;
    v_context                     rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PATIENT_NONDRUG_ALLERGIES';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PATIENT_NONDRUG_ALLERGIES', t_triggers => t_triggers);

    OPEN pt_nondrug_allergies_cur;

    OPEN pt_nondrug_allergies_rowid_cur;

    LOOP
      FETCH pt_nondrug_allergies_cur
      BULK COLLECT INTO t_pt_nondrug_allergies LIMIT bulk_limit;

      EXIT WHEN t_pt_nondrug_allergies.COUNT = 0;

      FETCH pt_nondrug_allergies_rowid_cur
      BULK COLLECT INTO t_pt_nondrug_allergies_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_pt_nondrug_allergies.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT patient_id
                             ,allergy
                             ,partnership_security_label
                             ,start_date
                             ,stop_date
                             ,xuser
                             ,xdate
                             ,comments
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,update_date
                         FROM thot.patient_nondrug_allergies)
               VALUES t_pt_nondrug_allergies(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PATIENT_NONDRUG_ALLERGIES'
                                                             , p_run_id => g_run_id);
            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_pt_nondrug_allergies.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                 INSERT INTO (SELECT patient_id
                             ,allergy
                             ,partnership_security_label
                             ,start_date
                             ,stop_date
                             ,xuser
                             ,xdate
                             ,comments
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,update_date
                         FROM thot.patient_nondrug_allergies)
                       VALUES t_pt_nondrug_allergies(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;
              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_pt_nondrug_allergies(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_pt_nondrug_allergies_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
               -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_pt_nondrug_allergies.COUNT || ' | Records inserted:' ||(t_pt_nondrug_allergies.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_pt_nondrug_allergies.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.patient_nondrug_allergies@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_pt_nondrug_allergies_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE pt_nondrug_allergies_cur;

    CLOSE pt_nondrug_allergies_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF pt_nondrug_allergies_cur%ISOPEN THEN
        CLOSE pt_nondrug_allergies_cur;
      END IF;

      IF pt_nondrug_allergies_rowid_cur%ISOPEN THEN
        CLOSE pt_nondrug_allergies_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_patient_nondrug_allergies;

--===================================================+
--| PULL_PATIENT_PHYSICIAN: Pulls PATIENT_PHYSICIAN table to rxhome.         |
--===================================================+
  PROCEDURE pull_patient_physician
  IS
    CURSOR patient_physician_cur
    IS
      SELECT   patient_id
              ,physician_id
              ,physician_seq
              ,active_flag
              ,created_by
              ,last_modified_by
              ,created_date
              ,last_modified_date
              ,partnership_security_label
              ,addr_seq
              ,phone_seq
          FROM rxh_stage.patient_physician@rxh_stg ppt
         WHERE ppt.processed_flag = 'N'
           AND ppt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = ppt.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_physician rxh
                           WHERE rxh.patient_id = ppt.patient_id
                             AND rxh.physician_id = ppt.physician_id)
      ORDER BY ROWID;

    CURSOR patient_physician_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.patient_physician@rxh_stg ppt
         WHERE ppt.processed_flag = 'N'
           AND ppt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = ppt.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_physician rxh
                           WHERE rxh.patient_id = ppt.patient_id
                             AND rxh.physician_id = ppt.physician_id)
      ORDER BY ROWID;

    TYPE patient_physician_tab IS TABLE OF patient_physician_cur%ROWTYPE;

    TYPE patient_physician_rowid_tab IS TABLE OF ROWID;

    t_patient_physician        patient_physician_tab;
    t_patient_physician_rowid  patient_physician_rowid_tab;
    t_triggers                 triggers_tab;
    v_context                  rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PATIENT_PHYSICIAN';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PATIENT_PHYSICIAN', t_triggers => t_triggers);

    OPEN patient_physician_cur;

    OPEN patient_physician_rowid_cur;

    LOOP
      FETCH patient_physician_cur
      BULK COLLECT INTO t_patient_physician LIMIT bulk_limit;

      EXIT WHEN t_patient_physician.COUNT = 0;

      FETCH patient_physician_rowid_cur
      BULK COLLECT INTO t_patient_physician_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_patient_physician.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT patient_id
                             ,physician_id
                             ,physician_seq
                             ,active_flag
                             ,created_by
                             ,last_modified_by
                             ,created_date
                             ,last_modified_date
                             ,partnership_security_label
                             ,addr_seq
                             ,phone_seq
                         FROM thot.patient_physician)
               VALUES t_patient_physician(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PATIENT_PHYSICIAN'
                                                             , p_run_id => g_run_id);

             -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_patient_physician.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
             g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                 INSERT INTO (SELECT patient_id
                             ,physician_id
                             ,physician_seq
                             ,active_flag
                             ,created_by
                             ,last_modified_by
                             ,created_date
                             ,last_modified_date
                             ,partnership_security_label
                             ,addr_seq
                             ,phone_seq
                         FROM thot.patient_physician)
               VALUES t_patient_physician(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

             rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_patient_physician(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_patient_physician_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_patient_physician.COUNT || ' | Records inserted:' ||(t_patient_physician.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_patient_physician.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.patient_physician@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_patient_physician_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE patient_physician_cur;

    CLOSE patient_physician_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF patient_physician_cur%ISOPEN THEN
        CLOSE patient_physician_cur;
      END IF;

      IF patient_physician_rowid_cur%ISOPEN THEN
        CLOSE patient_physician_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_patient_physician;


--=========================================================
--| PULL_PATIENT_MEASUREMENT: Pulls PATIENT_MEASUREMENT table to rxhome.        |
--=========================================================
  PROCEDURE pull_patient_measurement
  IS
    CURSOR patient_measurement_cur
    IS
      SELECT   patient_id
              ,date_measured
              ,height_inches
              ,height_cm
              ,weight_pounds
              ,weight_kg
              ,surface_area
              ,body_mass_index
              ,partnership_security_label
              ,creation_date
              ,created_by
              ,update_date
              ,updated_by
          FROM rxh_stage.patient_measurement@rxh_stg pmt
         WHERE pmt.processed_flag = 'N'
           AND pmt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = pmt.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_measurement rxh
                           WHERE rxh.patient_id = pmt.patient_id
                             AND rxh.date_measured = pmt.date_measured)
      ORDER BY ROWID;

    CURSOR patient_measurement_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.patient_measurement@rxh_stg pmt
         WHERE pmt.processed_flag = 'N'
           AND pmt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = pmt.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_measurement rxh
                           WHERE rxh.patient_id = pmt.patient_id
                             AND rxh.date_measured = pmt.date_measured)
      ORDER BY ROWID;

    TYPE patient_measurement_tab IS TABLE OF patient_measurement_cur%ROWTYPE;

    TYPE patient_measurement_rowid_tab IS TABLE OF ROWID;

    t_patient_measurement        patient_measurement_tab;
    t_patient_measurement_rowid  patient_measurement_rowid_tab;
    t_triggers                   triggers_tab;
    v_context                    rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PATIENT_MEASUREMENT';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PATIENT_MEASUREMENT', t_triggers => t_triggers);

    OPEN patient_measurement_cur;

    OPEN patient_measurement_rowid_cur;

    LOOP
      FETCH patient_measurement_cur
      BULK COLLECT INTO t_patient_measurement LIMIT bulk_limit;

      EXIT WHEN t_patient_measurement.COUNT = 0;

      FETCH patient_measurement_rowid_cur
      BULK COLLECT INTO t_patient_measurement_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_patient_measurement.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT patient_id
                             ,date_measured
                             ,height_inches
                             ,height_cm
                             ,weight_pounds
                             ,weight_kg
                             ,surface_area
                             ,body_mass_index
                             ,partnership_security_label
                             ,creation_date
                             ,created_by
                             ,update_date
                             ,updated_by
                         FROM thot.patient_measurement)
               VALUES t_patient_measurement(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PATIENT_MEASUREMENT'
                                                             , p_run_id => g_run_id);
             -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_patient_measurement.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
            g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                 INSERT INTO (SELECT patient_id
                             ,date_measured
                             ,height_inches
                             ,height_cm
                             ,weight_pounds
                             ,weight_kg
                             ,surface_area
                             ,body_mass_index
                             ,partnership_security_label
                             ,creation_date
                             ,created_by
                             ,update_date
                             ,updated_by
                         FROM thot.patient_measurement)
               VALUES t_patient_measurement(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;
              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_patient_measurement(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_patient_measurement_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_patient_measurement.COUNT || ' | Records inserted:' ||(t_patient_measurement.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_patient_measurement.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.patient_measurement@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_patient_measurement_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE patient_measurement_cur;

    CLOSE patient_measurement_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF patient_measurement_cur%ISOPEN THEN
        CLOSE patient_measurement_cur;
      END IF;

      IF patient_measurement_rowid_cur%ISOPEN THEN
        CLOSE patient_measurement_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_patient_measurement;


--===============================================
--| PULL_PATIENT_STATUS: Pulls PATIENT_STATUS table to rxhome.  |
--===============================================
  PROCEDURE pull_patient_status
  IS
    CURSOR patient_status_cur
    IS
      SELECT   patient_id
              ,status_type
              ,status
              ,status_date
              ,xuser
              ,therapy_id
              ,partnership_security_label
              ,created_by
              ,creation_date
              ,updated_by
              ,update_date
          FROM rxh_stage.patient_status@rxh_stg pst
         WHERE pst.processed_flag = 'N'
           AND pst.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = pst.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_status rxh
                           WHERE rxh.patient_id = pst.patient_id
                             AND rxh.status_type = pst.status_type
                             AND rxh.status_date = pst.status_date)
      ORDER BY ROWID;

    CURSOR patient_status_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.patient_status@rxh_stg pst
         WHERE pst.processed_flag = 'N'
           AND pst.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = pst.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_status rxh
                           WHERE rxh.patient_id = pst.patient_id
                             AND rxh.status_type = pst.status_type
                             AND rxh.status_date = pst.status_date)
      ORDER BY ROWID;

    TYPE patient_status_tab IS TABLE OF patient_status_cur%ROWTYPE;

    TYPE patient_status_rowid_tab IS TABLE OF ROWID;

    t_patient_status        patient_status_tab;
    t_patient_status_rowid  patient_status_rowid_tab;
    t_triggers              triggers_tab;
    v_context               rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PATIENT_STATUS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PATIENT_STATUS', t_triggers => t_triggers);

    OPEN patient_status_cur;

    OPEN patient_status_rowid_cur;

    LOOP
      FETCH patient_status_cur
      BULK COLLECT INTO t_patient_status LIMIT bulk_limit;

      EXIT WHEN t_patient_status.COUNT = 0;

      FETCH patient_status_rowid_cur
      BULK COLLECT INTO t_patient_status_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_patient_status.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT patient_id
                             ,status_type
                             ,status
                             ,status_date
                             ,xuser
                             ,therapy_id
                             ,partnership_security_label
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,update_date
                         FROM thot.patient_status)
               VALUES t_patient_status(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PATIENT_STATUS'
                                                             , p_run_id => g_run_id);
            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_patient_status.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                 INSERT INTO (SELECT patient_id
                             ,status_type
                             ,status
                             ,status_date
                             ,xuser
                             ,therapy_id
                             ,partnership_security_label
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,update_date
                         FROM thot.patient_status)
               VALUES t_patient_status(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_patient_status(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               ,p_attr2_name    => 'STATUS_TYPE'
                                                               ,p_attr2_value   => t_patient_status(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).status_type
                                                               ,p_attr3_name    => 'STATUS_DATE'
                                                               ,p_attr3_value   => t_patient_status(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).status_date
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_patient_status_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
               -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_patient_status.COUNT || ' | Records inserted:' ||(t_patient_status.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_patient_status.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.patient_status@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_patient_status_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE patient_status_cur;

    CLOSE patient_status_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF patient_status_cur%ISOPEN THEN
        CLOSE patient_status_cur;
      END IF;

      IF patient_status_rowid_cur%ISOPEN THEN
        CLOSE patient_status_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_patient_status;


--====================================================
--| PULL_PATIENT_THERAPIES: Pulls PATIENT_THERAPIES table to rxhome.  |
--====================================================
  PROCEDURE pull_patient_therapies
  IS
    CURSOR patient_therapies_cur
    IS
      SELECT   therapy_id
              ,patient_id
              ,therapy_type
              ,start_date
              ,stop_date
              ,stop_reason
              ,partnership_security_label
              ,status
              ,creation_date
              ,created_by
              ,update_date
              ,updated_by
              ,status_date
              ,status_seq_id
              ,data_sharing_authorization
              ,med_d_appeal_flag
              ,dsa_expiration_date  -- [ CQ23641 16.1 JCORONA : ADDING COLUMN dsa_expiration_date  TO THE PULL PROCESS
          FROM rxh_stage.patient_therapies@rxh_stg ptt
         WHERE ptt.processed_flag = 'N'
           AND ptt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = ptt.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_therapies rxh
                           WHERE rxh.therapy_id = ptt.therapy_id)
      ORDER BY ROWID;

    CURSOR patient_therapies_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.patient_therapies@rxh_stg ptt
         WHERE ptt.processed_flag = 'N'
           AND ptt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = ptt.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_therapies rxh
                           WHERE rxh.therapy_id = ptt.therapy_id)
      ORDER BY ROWID;

    TYPE patient_therapies_tab IS TABLE OF patient_therapies_cur%ROWTYPE;

    TYPE patient_therapies_rowid_tab IS TABLE OF ROWID;

    t_patient_therapies        patient_therapies_tab;
    t_patient_therapies_rowid  patient_therapies_rowid_tab;
    t_triggers                 triggers_tab;
    v_context                  rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PATIENT_THERAPIES';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PATIENT_THERAPIES', t_triggers => t_triggers);

    OPEN patient_therapies_cur;

    OPEN patient_therapies_rowid_cur;

    LOOP
      FETCH patient_therapies_cur
      BULK COLLECT INTO t_patient_therapies LIMIT bulk_limit;

      EXIT WHEN t_patient_therapies.COUNT = 0;

      FETCH patient_therapies_rowid_cur
      BULK COLLECT INTO t_patient_therapies_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_patient_therapies.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT therapy_id
                             ,patient_id
                             ,therapy_type
                             ,start_date
                             ,stop_date
                             ,stop_reason
                             ,partnership_security_label
                             ,status
                             ,creation_date
                             ,created_by
                             ,update_date
                             ,updated_by
                             ,status_date
                             ,status_seq_id
                             ,data_sharing_authorization
                             ,med_d_appeal_flag
                             ,dsa_expiration_date  -- [CQ23641 16.1 JCORONA  : ADDING COLUMN dsa_expiration_date  TO THE PULL PROCESS
                         FROM thot.patient_therapies)
               VALUES t_patient_therapies(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PATIENT_THERAPIES'
                                                             , p_run_id => g_run_id);
            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_patient_therapies.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                INSERT INTO (SELECT therapy_id
                             ,patient_id
                             ,therapy_type
                             ,start_date
                             ,stop_date
                             ,stop_reason
                             ,partnership_security_label
                             ,status
                             ,creation_date
                             ,created_by
                             ,update_date
                             ,updated_by
                             ,status_date
                             ,status_seq_id
                             ,data_sharing_authorization
                             ,med_d_appeal_flag
                             ,dsa_expiration_date  -- [CQ23641 16.1 JCORONA : ADDING COLUMN dsa_expiration_date  TO THE PULL PROCESS
                         FROM thot.patient_therapies)
               VALUES t_patient_therapies(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;
              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_patient_therapies(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_patient_therapies_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_patient_therapies.COUNT || ' | Records inserted:' ||(t_patient_therapies.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_patient_therapies.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.patient_therapies@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_patient_therapies_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE patient_therapies_cur;

    CLOSE patient_therapies_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF patient_therapies_cur%ISOPEN THEN
        CLOSE patient_therapies_cur;
      END IF;

      IF patient_therapies_rowid_cur%ISOPEN THEN
        CLOSE patient_therapies_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_patient_therapies;


--==================================
--| PULL_CHARTS: Pulls CHARTS table to rxhome.  |
--==================================
  PROCEDURE pull_charts
  IS
    CURSOR charts_cur
    IS
      SELECT  rowid row_id
              ,commnote_type
              ,xuser
              ,xdate
              ,xtime
              ,patient_id
              ,claimctr_id
              ,physician_id
              ,hospital_id
              ,nagency_id
              ,refsource_type
              ,refsource_id
              ,text
              ,svcbr_id
              ,prescription_id
              ,refill_no
              ,nurse_id
              ,start_time
              ,invoice_id
              ,invoice_seq
              ,physician_signature_flag
              ,followup_date
              ,chart_svcbr_id
              ,system_generated_flag
              ,notetype_id
              ,followup_completed_flag
              ,partnership_security_label
              ,chart_id
              ,therapy_type
              ,charts_seq_id
              ,call_type_id
              ,phys_addr_seq
              ,contact_type
              ,created_by
              ,creation_date
              ,uniq_id
          FROM rxh_stage.charts@rxh_stg cht
         WHERE cht.processed_flag = 'N'
           AND cht.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = cht.patient_id);

 -- CQ24083 jarcega Added code starts here
   TYPE c1_rowid_tab            IS TABLE OF ROWID;
   TYPE c1_commnote_type_tab    IS TABLE OF VARCHAR2(5);
   TYPE c1_xuser_tab            IS TABLE OF VARCHAR2(30);
   TYPE c1_XDATE_tab            IS TABLE OF DATE;
   TYPE c1_XTIME_tab            IS TABLE OF VARCHAR2(8);
   TYPE c1_patient_id_tab	      IS TABLE OF number(10);
   TYPE c1_claimctr_id_tab	    IS TABLE OF number(10);
   TYPE c1_physician_id_tab	    IS TABLE OF number(10);
   TYPE c1_hospital_id_tab	    IS TABLE OF number(10);
   TYPE c1_nagency_id_tab	      IS TABLE OF number(10);
   TYPE c1_refsource_type_tab	  IS TABLE OF varchar2(1);
   TYPE c1_refsource_id_tab	    IS TABLE OF number(10);
   TYPE c1_text_tab	            IS TABLE OF varchar2(4000);
   TYPE c1_svcbr_id_tab	        IS TABLE OF number(3);
   TYPE c1_prescription_id_tab	IS TABLE OF number(10);
   TYPE c1_refill_no_tab	      IS TABLE OF number(2);
   TYPE c1_nurse_id_tab	        IS TABLE OF number(10);
   TYPE c1_start_time_tab	      IS TABLE OF date;
   TYPE c1_invoice_id_tab	      IS TABLE OF number(10);
   TYPE c1_invoice_seq_tab	    IS TABLE OF number(2);
   TYPE c1_phys_sign_flag_tab	  IS TABLE OF varchar2(1);
   TYPE c1_followup_date_tab	  IS TABLE OF date;
   TYPE c1_chart_svcbr_id_tab	  IS TABLE OF number(3);
   TYPE c1_syst_gene_flag_tab	  IS TABLE OF varchar2(1);
   TYPE c1_notetype_id_tab	    IS TABLE OF number(10);
   TYPE c1_foll_comp_flag_tab	  IS TABLE OF varchar2(1);
   TYPE c1_part_secu_label_tab	IS TABLE OF number(10);
   TYPE c1_chart_id_tab	        IS TABLE OF number;
   TYPE c1_therapy_type_tab	    IS TABLE OF varchar2(4);
   TYPE c1_charts_seq_id_tab	  IS TABLE OF number(10);
   TYPE c1_call_type_id_tab	    IS TABLE OF number;
   TYPE c1_phys_addr_seq_tab	  IS TABLE OF number(5);
   TYPE c1_contact_type_tab	    IS TABLE OF varchar2(1);
   TYPE c1_created_by           IS TABLE OF varchar2(30);
   TYPE c1_creation_date        IS TABLE OF date;
   TYPE c1_uniq_id              IS TABLE OF number(15);


    t_rowid             c1_rowid_tab;
    t_commnote_type     c1_commnote_type_tab;
    t_xuser             c1_xuser_tab;
    t_XDATE             c1_XDATE_tab;
    t_XTIME             c1_XTIME_tab;
    t_patient_id        c1_patient_id_tab;
    t_claimctr_id       c1_claimctr_id_tab;
    t_physician_id      c1_physician_id_tab;
    t_hospital_id       c1_hospital_id_tab;
    t_nagency_id        c1_nagency_id_tab;
    t_refsource_type    c1_refsource_type_tab;
    t_refsource_id      c1_refsource_id_tab;
    t_text              c1_text_tab;
    t_svcbr_id          c1_svcbr_id_tab;
    t_prescription_id   c1_prescription_id_tab;
    t_refill_no         c1_refill_no_tab;
    t_nurse_id          c1_nurse_id_tab;
    t_start_time        c1_start_time_tab;
    t_invoice_id        c1_invoice_id_tab;
    t_invoice_seq       c1_invoice_seq_tab;
    t_phys_sign_flag    c1_phys_sign_flag_tab;
    t_followup_date     c1_followup_date_tab;
    t_chart_svcbr_id    c1_chart_svcbr_id_tab;
    t_syst_gene_flag    c1_syst_gene_flag_tab;
    t_notetype_id       c1_notetype_id_tab;
    t_foll_comp_flag    c1_foll_comp_flag_tab;
    t_part_secu_label   c1_part_secu_label_tab;
    t_chart_id          c1_chart_id_tab;
    t_therapy_type      c1_therapy_type_tab;
    t_charts_seq_id     c1_charts_seq_id_tab;
    t_call_type_id      c1_call_type_id_tab;
    t_phys_addr_seq     c1_phys_addr_seq_tab;
    t_contact_type      c1_contact_type_tab;
    t_created_by        c1_created_by;
    t_creation_date     c1_creation_date;
    t_uniq_id           c1_uniq_id;

-- CQ24083 jarcega Added code Ends here


   t_triggers      triggers_tab;
   v_context       rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_CHARTS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'CHARTS', t_triggers => t_triggers);

    OPEN charts_cur;

    LOOP
      FETCH charts_cur
      BULK COLLECT INTO t_rowid
                        ,t_commnote_type
                        ,t_xuser
                        ,t_XDATE
                        ,t_XTIME
                        ,t_patient_id
                        ,t_claimctr_id
                        ,t_physician_id
                        ,t_hospital_id
                        ,t_nagency_id
                        ,t_refsource_type
                        ,t_refsource_id
                        ,t_text
                        ,t_svcbr_id
                        ,t_prescription_id
                        ,t_refill_no
                        ,t_nurse_id
                        ,t_start_time
                        ,t_invoice_id
                        ,t_invoice_seq
                        ,t_phys_sign_flag
                        ,t_followup_date
                        ,t_chart_svcbr_id
                        ,t_syst_gene_flag
                        ,t_notetype_id
                        ,t_foll_comp_flag
                        ,t_part_secu_label
                        ,t_chart_id
                        ,t_therapy_type
                        ,t_charts_seq_id
                        ,t_call_type_id
                        ,t_phys_addr_seq
                        ,t_contact_type
                        ,t_created_by
                        ,t_creation_date
                        ,t_uniq_id
       LIMIT bulk_limit;

      EXIT WHEN t_rowid.COUNT = 0;

-- CQ24083 jarcega Changed code in order to use the tab variables.

      BEGIN
        FORALL i IN 1 .. t_rowid.COUNT SAVE EXCEPTIONS
          INSERT INTO thot.charts (commnote_type
                                   ,xuser
                                   ,xdate
                                   ,xtime
                                   ,patient_id
                                   ,claimctr_id
                                   ,physician_id
                                   ,hospital_id
                                   ,nagency_id
                                   ,refsource_type
                                   ,refsource_id
                                   ,text
                                   ,svcbr_id
                                   ,prescription_id
                                   ,refill_no
                                   ,nurse_id
                                   ,start_time
                                   ,invoice_id
                                   ,invoice_seq
                                   ,physician_signature_flag
                                   ,followup_date
                                   ,chart_svcbr_id
                                   ,system_generated_flag
                                   ,notetype_id
                                   ,followup_completed_flag
                                   ,partnership_security_label
                                   ,chart_id
                                   ,therapy_type
                                   ,charts_seq_id
                                   ,call_type_id
                                   ,phys_addr_seq
                                   ,contact_type
                                   ,created_by
                                   ,creation_date
                                   ,uniq_id)
                           VALUES ( t_commnote_type(i)
                                   ,t_xuser(i)
                                   ,t_XDATE(i)
                                   ,t_XTIME(i)
                                   ,t_patient_id(i)
                                   ,t_claimctr_id(i)
                                   ,t_physician_id(i)
                                   ,t_hospital_id(i)
                                   ,t_nagency_id(i)
                                   ,t_refsource_type(i)
                                   ,t_refsource_id(i)
                                   ,t_text(i)
                                   ,t_svcbr_id(i)
                                   ,t_prescription_id(i)
                                   ,t_refill_no(i)
                                   ,t_nurse_id(i)
                                   ,t_start_time(i)
                                   ,t_invoice_id(i)
                                   ,t_invoice_seq(i)
                                   ,t_phys_sign_flag(i)
                                   ,t_followup_date(i)
                                   ,t_chart_svcbr_id(i)
                                   ,t_syst_gene_flag(i)
                                   ,t_notetype_id(i)
                                   ,t_foll_comp_flag(i)
                                   ,t_part_secu_label(i)
                                   ,t_chart_id(i)
                                   ,t_therapy_type(i)
                                   ,t_charts_seq_id(i)
                                   ,t_call_type_id(i)
                                   ,t_phys_addr_seq(i)
                                   ,t_contact_type(i)
                                   ,t_created_by(i)
                                   ,t_creation_date(i)
                                   ,t_uniq_id(i));
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.CHARTS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_rowid.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --

          -- CQ24083 jarcega Changed code in order to use the tab variables.

          INSERT INTO thot.charts (commnote_type
                                   ,xuser
                                   ,xdate
                                   ,xtime
                                   ,patient_id
                                   ,claimctr_id
                                   ,physician_id
                                   ,hospital_id
                                   ,nagency_id
                                   ,refsource_type
                                   ,refsource_id
                                   ,text
                                   ,svcbr_id
                                   ,prescription_id
                                   ,refill_no
                                   ,nurse_id
                                   ,start_time
                                   ,invoice_id
                                   ,invoice_seq
                                   ,physician_signature_flag
                                   ,followup_date
                                   ,chart_svcbr_id
                                   ,system_generated_flag
                                   ,notetype_id
                                   ,followup_completed_flag
                                   ,partnership_security_label
                                   ,chart_id
                                   ,therapy_type
                                   ,charts_seq_id
                                   ,call_type_id
                                   ,phys_addr_seq
                                   ,contact_type
                                   ,created_by
                                   ,creation_date
                                   ,uniq_id)
                           VALUES ( t_commnote_type(g_index_err)
                                   ,t_xuser(g_index_err)
                                   ,t_XDATE(g_index_err)
                                   ,t_XTIME(g_index_err)
                                   ,t_patient_id(g_index_err)
                                   ,t_claimctr_id(g_index_err)
                                   ,t_physician_id(g_index_err)
                                   ,t_hospital_id(g_index_err)
                                   ,t_nagency_id(g_index_err)
                                   ,t_refsource_type(g_index_err)
                                   ,t_refsource_id(g_index_err)
                                   ,t_text(g_index_err)
                                   ,t_svcbr_id(g_index_err)
                                   ,t_prescription_id(g_index_err)
                                   ,t_refill_no(g_index_err)
                                   ,t_nurse_id(g_index_err)
                                   ,t_start_time(g_index_err)
                                   ,t_invoice_id(g_index_err)
                                   ,t_invoice_seq(g_index_err)
                                   ,t_phys_sign_flag(g_index_err)
                                   ,t_followup_date(g_index_err)
                                   ,t_chart_svcbr_id(g_index_err)
                                   ,t_syst_gene_flag(g_index_err)
                                   ,t_notetype_id(g_index_err)
                                   ,t_foll_comp_flag(g_index_err)
                                   ,t_part_secu_label(g_index_err)
                                   ,t_chart_id(g_index_err)
                                   ,t_therapy_type(g_index_err)
                                   ,t_charts_seq_id(g_index_err)
                                   ,t_call_type_id(g_index_err)
                                   ,t_phys_addr_seq(g_index_err)
                                   ,t_contact_type(g_index_err)
                                   ,t_created_by(g_index_err)
                                   ,t_creation_date(g_index_err)
                                   ,t_uniq_id(g_index_err) );

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_patient_id(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_rowid.COUNT || ' | Records inserted:' ||(t_rowid.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_rowid.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.charts@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_rowid(i);  -- CQ24083 jarcega Changed code in order to use the tab variable t_rowid.
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE charts_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF charts_cur%ISOPEN THEN
        CLOSE charts_cur;
      END IF;



      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_charts;

--=====================================================
--| PULL_PATIENT_REFSOURCE: Pulls PATIENT_REFSOURCE table to rxhome.  |
--=====================================================
  PROCEDURE pull_patient_refsource
  IS
    CURSOR patient_refsource_cur
    IS
      SELECT   patient_id
              ,refsource_type
              ,refsource_id
              ,referral_date
              ,contact_xuser
              ,partnership_security_label
              ,referral_method
              ,created_by
              ,creation_date
              ,updated_by
              ,updated_date
              ,ccs_referral_name
          FROM rxh_stage.patient_refsource@rxh_stg prt
         WHERE prt.processed_flag = 'N'
           AND prt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = prt.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_refsource rxh
                           WHERE rxh.patient_id = prt.patient_id)
      ORDER BY ROWID;

    CURSOR patient_refsource_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.patient_refsource@rxh_stg prt
         WHERE prt.processed_flag = 'N'
           AND prt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = prt.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_refsource rxh
                           WHERE rxh.patient_id = prt.patient_id)
      ORDER BY ROWID;

    TYPE patient_refsource_tab IS TABLE OF patient_refsource_cur%ROWTYPE;

    TYPE patient_refsource_rowid_tab IS TABLE OF ROWID;

    t_patient_refsource        patient_refsource_tab;
    t_patient_refsource_rowid  patient_refsource_rowid_tab;
    t_triggers                 triggers_tab;
    v_context                  rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PATIENT_REFSOURCE';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PATIENT_REFSOURCE', t_triggers => t_triggers);

    OPEN patient_refsource_cur;

    OPEN patient_refsource_rowid_cur;

    LOOP
      FETCH patient_refsource_cur
      BULK COLLECT INTO t_patient_refsource LIMIT bulk_limit;

      EXIT WHEN t_patient_refsource.COUNT = 0;

      FETCH patient_refsource_rowid_cur
      BULK COLLECT INTO t_patient_refsource_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_patient_refsource.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT patient_id
                             ,refsource_type
                             ,refsource_id
                             ,referral_date
                             ,contact_xuser
                             ,partnership_security_label
                             ,referral_method
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,updated_date
                             ,ccs_referral_name
                         FROM thot.patient_refsource)
               VALUES t_patient_refsource(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PATIENT_REFSOURCE'
                                                             , p_run_id => g_run_id);
            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_patient_refsource.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                INSERT INTO (SELECT patient_id
                             ,refsource_type
                             ,refsource_id
                             ,referral_date
                             ,contact_xuser
                             ,partnership_security_label
                             ,referral_method
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,updated_date
                             ,ccs_referral_name
                         FROM thot.patient_refsource)
               VALUES t_patient_refsource(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;
              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_patient_refsource(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_patient_refsource_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_patient_refsource.COUNT || ' | Records inserted:' ||(t_patient_refsource.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_patient_refsource.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.patient_refsource@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_patient_refsource_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE patient_refsource_cur;

    CLOSE patient_refsource_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF patient_refsource_cur%ISOPEN THEN
        CLOSE patient_refsource_cur;
      END IF;

      IF patient_refsource_rowid_cur%ISOPEN THEN
        CLOSE patient_refsource_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_patient_refsource;

--============================================
--| PULL_ASSIGNED_IDS: Pulls ASSIGNED_IDS table to rxhome.      |
--============================================
  PROCEDURE pull_assigned_ids
  IS
    CURSOR assigned_ids_cur
    IS
      SELECT   assigner_type
              ,assigner_id
              ,assignee_type
              ,assignee_id
              ,assigned_id
              ,xuser
              ,xdate
              ,created_by
              ,creation_date
              ,updated_by
              ,updated_date
              ,assigned_id_rule_overwrite
          FROM rxh_stage.assigned_ids@rxh_stg ast
         WHERE ast.processed_flag = 'N'
           AND ast.run_id = g_run_id
           AND ast.assignee_type = 'P'
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = ast.assignee_id
                         AND ast.assignee_type = 'P'
                     )

           AND NOT EXISTS(SELECT 1
                            FROM thot.assigned_ids rxh
                           WHERE rxh.assigner_type = ast.assigner_type
                             AND rxh.assigner_id = ast.assigner_id
                             AND rxh.assigned_id = ast.assigned_id
                             AND rxh.assignee_type = ast.assignee_type
                             AND rxh.assignee_id = ast.assignee_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.assigned_ids rxh
                           WHERE rxh.assigner_type = ast.assigner_type
                             AND rxh.assigner_id = ast.assigner_id
                             AND rxh.assigned_id = ast.assigned_id
                          )
      ORDER BY ROWID;

    CURSOR assigned_ids_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.assigned_ids@rxh_stg ast
         WHERE ast.assignee_type = 'P'
           AND ast.processed_flag = 'N'
           AND ast.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = ast.assignee_id
                         AND ast.assignee_type = 'P')
           AND NOT EXISTS(SELECT 1
                         FROM thot.assigned_ids rxh
                        WHERE rxh.assigner_type = ast.assigner_type
                          AND rxh.assigner_id = ast.assigner_id
                          AND rxh.assigned_id = ast.assigned_id
                          AND rxh.assignee_type = ast.assignee_type
                          AND rxh.assignee_id = ast.assignee_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.assigned_ids rxh
                           WHERE rxh.assigner_type = ast.assigner_type
                             AND rxh.assigner_id = ast.assigner_id
                             AND rxh.assigned_id = ast.assigned_id
                          )
      ORDER BY ROWID;

    TYPE assigned_ids_tab IS TABLE OF assigned_ids_cur%ROWTYPE;

    TYPE assigned_ids_rowid_tab IS TABLE OF ROWID;

    t_assigned_ids        assigned_ids_tab;
    t_assigned_ids_rowid  assigned_ids_rowid_tab;
    t_triggers                   triggers_tab;
    v_context             rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_ASSIGNED_IDS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'ASSIGNED_IDS', t_triggers => t_triggers);

    OPEN assigned_ids_cur;

    OPEN assigned_ids_rowid_cur;

    LOOP
      FETCH assigned_ids_cur
      BULK COLLECT INTO t_assigned_ids LIMIT bulk_limit;

      EXIT WHEN t_assigned_ids.COUNT = 0;

      FETCH assigned_ids_rowid_cur
      BULK COLLECT INTO t_assigned_ids_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_assigned_ids.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT assigner_type
                             ,assigner_id
                             ,assignee_type
                             ,assignee_id
                             ,assigned_id
                             ,xuser
                             ,xdate
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,updated_date
                             ,assigned_id_rule_overwrite
                         FROM thot.assigned_ids)
               VALUES t_assigned_ids(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.ASSIGNED_IDS'
                                                             , p_run_id => g_run_id);
             -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_assigned_ids.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                 INSERT INTO (SELECT assigner_type
                             ,assigner_id
                             ,assignee_type
                             ,assignee_id
                             ,assigned_id
                             ,xuser
                             ,xdate
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,updated_date
                             ,assigned_id_rule_overwrite
                         FROM thot.assigned_ids)
               VALUES t_assigned_ids(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;
              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_assigned_ids(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).assignee_id
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_assigned_ids_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_assigned_ids.COUNT || ' | Records inserted:' ||(t_assigned_ids.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_assigned_ids.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.assigned_ids@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_assigned_ids_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE assigned_ids_cur;

    CLOSE assigned_ids_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF assigned_ids_cur%ISOPEN THEN
        CLOSE assigned_ids_cur;
      END IF;

      IF assigned_ids_rowid_cur%ISOPEN THEN
        CLOSE assigned_ids_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_assigned_ids;
--============================================
--| PULL_MED_PROFILE: Pulls MED_PROFILE table to rxhome.           |
--============================================
  PROCEDURE pull_med_profile
  IS
    CURSOR med_profile_cur
    IS
      SELECT   patient_id
              ,prof_type
              ,prof_id
              ,description
              ,pta_flag
              ,drug_route
              ,dosage
              ,dosage_unit
              ,freq
              ,start_date
              ,stop_date
              ,comments
              ,xuser
              ,create_date
              ,mod_date
              ,mod_flag
              ,outside_source_flag
              ,svcbr_id
              ,prescription_id
              ,refill_no
              ,partnership_security_label
              ,rxnc
              ,drug_type
          FROM rxh_stage.med_profile@rxh_stg mpt
         WHERE mpt.processed_flag = 'N'
           AND mpt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = mpt.patient_id)
      ORDER BY ROWID;

    CURSOR med_profile_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.med_profile@rxh_stg mpt
         WHERE mpt.processed_flag = 'N'
           AND mpt.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = mpt.patient_id)
      ORDER BY ROWID;

    TYPE med_profile_tab IS TABLE OF med_profile_cur%ROWTYPE;

    TYPE med_profile_rowid_tab IS TABLE OF ROWID;

    t_med_profile        med_profile_tab;
    t_med_profile_rowid  med_profile_rowid_tab;
    t_triggers           triggers_tab;
    v_context            rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_MED_PROFILE';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'MED_PROFILE', t_triggers => t_triggers);

    OPEN med_profile_cur;

    OPEN med_profile_rowid_cur;

    LOOP
      FETCH med_profile_cur
      BULK COLLECT INTO t_med_profile LIMIT bulk_limit;

      EXIT WHEN t_med_profile.COUNT = 0;

      FETCH med_profile_rowid_cur
      BULK COLLECT INTO t_med_profile_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_med_profile.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT patient_id
                             ,prof_type
                             ,prof_id
                             ,description
                             ,pta_flag
                             ,drug_route
                             ,dosage
                             ,dosage_unit
                             ,freq
                             ,start_date
                             ,stop_date
                             ,comments
                             ,xuser
                             ,create_date
                             ,mod_date
                             ,mod_flag
                             ,outside_source_flag
                             ,svcbr_id
                             ,prescription_id
                             ,refill_no
                             ,partnership_security_label
                             ,rxnc
                             ,drug_type
                         FROM thot.med_profile)
               VALUES t_med_profile(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.MED_PROFILE'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_med_profile.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT patient_id
                                   ,prof_type
                                   ,prof_id
                                   ,description
                                   ,pta_flag
                                   ,drug_route
                                   ,dosage
                                   ,dosage_unit
                                   ,freq
                                   ,start_date
                                   ,stop_date
                                   ,comments
                                   ,xuser
                                   ,create_date
                                   ,mod_date
                                   ,mod_flag
                                   ,outside_source_flag
                                   ,svcbr_id
                                   ,prescription_id
                                   ,refill_no
                                   ,partnership_security_label
                                   ,rxnc
                                   ,drug_type
                               FROM thot.med_profile)
                     VALUES t_med_profile(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_med_profile(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_med_profile_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_med_profile.COUNT || ' | Records inserted:' ||(t_med_profile.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_med_profile.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.med_profile@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_med_profile_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE med_profile_cur;

    CLOSE med_profile_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF med_profile_cur%ISOPEN THEN
        CLOSE med_profile_cur;
      END IF;

      IF med_profile_rowid_cur%ISOPEN THEN
        CLOSE med_profile_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_med_profile;

--=============================================
--| PULL_CREDIT_CARDS: Pulls CREDIT_CARDS table to rxhome.   |
--=============================================
  PROCEDURE pull_credit_cards
  IS
    CURSOR credit_cards_cur
    IS
      SELECT   card_key_id
              ,card_issuer_id
              ,card_type
              ,thot.cc_crypto.cc_encrypt_wrapper(card_number) as card_number -->>CQ22670
              ,card_holder
              ,card_id_number
              ,card_exp_month
              ,card_exp_year
              ,name_type
              ,name_id
              ,addr_seq
              ,use_type
              ,use_card_from
              ,use_card_to
              ,max_payment
              ,max_payment_unit
              ,last_4_digits
              ,comments
              ,created_by
              ,creation_date
              ,updated_by
              ,updated_date
          FROM rxh_stage.credit_cards@rxh_stg cct
         WHERE cct.processed_flag = 'N'
           AND cct.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = cct.name_id)
           AND EXISTS(SELECT 1
                        FROM thot.addresses rxh
                       WHERE rxh.name_type = cct.name_type
                         AND rxh.name_id = cct.name_id
                         AND rxh.addr_seq = cct.addr_seq)
      ORDER BY card_key_id;

    CURSOR credit_cards_rowid_cur
    IS
      SELECT   card_key_id
          FROM rxh_stage.credit_cards@rxh_stg cct
         WHERE cct.processed_flag = 'N'
           AND cct.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = cct.name_id)
           AND EXISTS(SELECT 1
                        FROM thot.addresses rxh
                       WHERE rxh.name_type = cct.name_type
                         AND rxh.name_id = cct.name_id
                         AND rxh.addr_seq = cct.addr_seq)
      ORDER BY card_key_id;

    TYPE credit_cards_tab IS TABLE OF credit_cards_cur%ROWTYPE;

    TYPE credit_cards_rowid_tab IS TABLE OF credit_cards_rowid_cur%ROWTYPE;

    t_credit_cards        credit_cards_tab;
    t_credit_cards_rowid  credit_cards_rowid_tab;
    t_triggers            triggers_tab;
    v_context             rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_CREDIT_CARDS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'CREDIT_CARDS', t_triggers => t_triggers);

    OPEN credit_cards_cur;

    OPEN credit_cards_rowid_cur;

    LOOP
      FETCH credit_cards_cur
      BULK COLLECT INTO t_credit_cards LIMIT bulk_limit;

      EXIT WHEN t_credit_cards.COUNT = 0;

      FETCH credit_cards_rowid_cur
      BULK COLLECT INTO t_credit_cards_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_credit_cards.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT card_key_id
                             ,card_issuer_id
                             ,card_type
                             ,card_number
                             ,card_holder
                             ,card_id_number
                             ,card_exp_month
                             ,card_exp_year
                             ,name_type
                             ,name_id
                             ,addr_seq
                             ,use_type
                             ,use_card_from
                             ,use_card_to
                             ,max_payment
                             ,max_payment_unit
                             ,last_4_digits
                             ,comments
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,updated_date
                         FROM thot.credit_cards)
               VALUES t_credit_cards(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.CREDIT_CARDS'
                                                             , p_run_id => g_run_id);
            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_credit_cards.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                 INSERT INTO (SELECT card_key_id
                             ,card_issuer_id
                             ,card_type
                             ,card_number
                             ,card_holder
                             ,card_id_number
                             ,card_exp_month
                             ,card_exp_year
                             ,name_type
                             ,name_id
                             ,addr_seq
                             ,use_type
                             ,use_card_from
                             ,use_card_to
                             ,max_payment
                             ,max_payment_unit
                             ,last_4_digits
                             ,comments
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,updated_date
                         FROM thot.credit_cards)
               VALUES t_credit_cards(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;
              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_credit_cards(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).name_id
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_credit_cards_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
               -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_credit_cards.COUNT || ' | Records inserted:' ||(t_credit_cards.COUNT - SQL%BULK_EXCEPTIONS.COUNT), p_run_id => g_run_id);

      FOR i IN 1 .. t_credit_cards.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.credit_cards@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE card_key_id = t_credit_cards_rowid(i).card_key_id;
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE credit_cards_cur;

    CLOSE credit_cards_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF credit_cards_cur%ISOPEN THEN
        CLOSE credit_cards_cur;
      END IF;

      IF credit_cards_rowid_cur%ISOPEN THEN
        CLOSE credit_cards_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_credit_cards;

--========================================
--| PULL_PHYSICIANS: Pulls PHYSICIANS table to rxhome.       |
--========================================
  PROCEDURE pull_physicians
  IS
    CURSOR physicians_cur
    IS
      SELECT   ID
              ,LAST
              ,FIRST
              ,mi
              ,mon_of_birth
              ,day_of_birth
              ,yr_of_birth
              ,specialty_code
              ,license_id
              ,dea_id
              ,upin
              ,addr1
              ,addr2
              ,city
              ,state
              ,zip1
              ,zip2
              ,country
              ,comments
              ,addr_label_printok
              ,tax_id
              ,medicaid_id
              ,socsec
              ,employment_start_date
              ,employment_stop_date
              ,xuser
              ,invoicer
              ,marketing_id
              ,pricing_id
              ,status
              ,phy_credential
              ,inactive_date
              ,inactivated_by
              ,creation_date
              ,created_by
              ,update_date
              ,updated_by
              ,npi_id
              ,status_dea_id
          FROM rxh_stage.physicians@rxh_stg phy
         WHERE phy.processed_flag = 'N'
           AND phy.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.physicians rxh
                           WHERE rxh.ID = phy.ID)
      ORDER BY ROWID;

    CURSOR physicians_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.physicians@rxh_stg phy
         WHERE phy.processed_flag = 'N'
           AND phy.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.physicians rxh
                           WHERE rxh.ID = phy.ID)
      ORDER BY ROWID;

    TYPE physicians_tab IS TABLE OF physicians_cur%ROWTYPE;

    TYPE physicians_rowid_tab IS TABLE OF ROWID;

    t_physicians        physicians_tab;
    t_physicians_rowid  physicians_rowid_tab;
    t_triggers          triggers_tab;
    v_context           rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PHYSICIANS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PHYSICIANS', t_triggers => t_triggers);

    OPEN physicians_cur;

    OPEN physicians_rowid_cur;

    LOOP
      FETCH physicians_cur
      BULK COLLECT INTO t_physicians LIMIT bulk_limit;

      EXIT WHEN t_physicians.COUNT = 0;

      FETCH physicians_rowid_cur
      BULK COLLECT INTO t_physicians_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_physicians.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT ID
                             ,LAST
                             ,FIRST
                             ,mi
                             ,mon_of_birth
                             ,day_of_birth
                             ,yr_of_birth
                             ,specialty_code
                             ,license_id
                             ,dea_id
                             ,upin
                             ,addr1
                             ,addr2
                             ,city
                             ,state
                             ,zip1
                             ,zip2
                             ,country
                             ,comments
                             ,addr_label_printok
                             ,tax_id
                             ,medicaid_id
                             ,socsec
                             ,employment_start_date
                             ,employment_stop_date
                             ,xuser
                             ,invoicer
                             ,marketing_id
                             ,pricing_id
                             ,status
                             ,phy_credential
                             ,inactive_date
                             ,inactivated_by
                             ,creation_date
                             ,created_by
                             ,update_date
                             ,updated_by
                             ,npi_id
                             ,status_dea_id
                         FROM thot.physicians)
               VALUES t_physicians(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PHYSICIANS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_physicians.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT ID
                                   ,LAST
                                   ,FIRST
                                   ,mi
                                   ,mon_of_birth
                                   ,day_of_birth
                                   ,yr_of_birth
                                   ,specialty_code
                                   ,license_id
                                   ,dea_id
                                   ,upin
                                   ,addr1
                                   ,addr2
                                   ,city
                                   ,state
                                   ,zip1
                                   ,zip2
                                   ,country
                                   ,comments
                                   ,addr_label_printok
                                   ,tax_id
                                   ,medicaid_id
                                   ,socsec
                                   ,employment_start_date
                                   ,employment_stop_date
                                   ,xuser
                                   ,invoicer
                                   ,marketing_id
                                   ,pricing_id
                                   ,status
                                   ,phy_credential
                                   ,inactive_date
                                   ,inactivated_by
                                   ,creation_date
                                   ,created_by
                                   ,update_date
                                   ,updated_by
                                   ,npi_id
                                   ,status_dea_id
                               FROM thot.physicians)
                     VALUES t_physicians(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'PHYSICIAN_ID'
                                                               ,p_attr1_value   => t_physicians(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).ID
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_physicians_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_physicians.COUNT || ' | Records inserted:' ||(t_physicians.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_physicians.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.physicians@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_physicians_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE physicians_cur;

    CLOSE physicians_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF physicians_cur%ISOPEN THEN
        CLOSE physicians_cur;
      END IF;

      IF physicians_rowid_cur%ISOPEN THEN
        CLOSE physicians_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_physicians;

--========================================
--| PULL_ATTRIBUTES: Pulls ATTRIBUTES table to rxhome.     |
--========================================
  PROCEDURE pull_attributes
  IS
    CURSOR attributes_cur
    IS
      SELECT   name_type
              ,name_id
              ,attribute_type
              ,ATTRIBUTE
              ,effective_date
              ,expiration_date
              ,state
              ,partnership_security_label
              ,status
              ,comments
              ,validated_by
              ,validation_date
              ,user_id
              ,attribute1
              ,attribute2
              ,attribute3
              ,attribute4
              ,attribute5
              ,attribute6
              ,attribute7
              ,attribute8
              ,attribute9
              ,attribute10
              ,attribute11
              ,attribute12
              ,attribute13
              ,attribute14
              ,attribute15
              ,created_by
              ,creation_date
              ,updated_by
              ,updated_date
          FROM rxh_stage.ATTRIBUTES@rxh_stg att
         WHERE att.processed_flag = 'N'
           AND att.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.physicians rxh
                       WHERE rxh.ID = att.name_id
                         AND att.name_type = 'D')
           AND NOT EXISTS(SELECT 1
                            FROM thot.ATTRIBUTES rxh
                           WHERE rxh.name_type = att.name_type
                             AND rxh.name_id = att.name_id
                             AND rxh.attribute_type = att.attribute_type
                             AND rxh.state = att.state)
      ORDER BY ROWID;

    CURSOR attributes_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.ATTRIBUTES@rxh_stg att
         WHERE att.processed_flag = 'N'
           AND att.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.physicians rxh
                       WHERE rxh.ID = att.name_id
                         AND att.name_type = 'D')
           AND NOT EXISTS(SELECT 1
                            FROM thot.ATTRIBUTES rxh
                           WHERE rxh.name_type = att.name_type
                             AND rxh.name_id = att.name_id
                             AND rxh.attribute_type = att.attribute_type
                             AND rxh.state = att.state)
      ORDER BY ROWID;

    TYPE attributes_tab IS TABLE OF attributes_cur%ROWTYPE;

    TYPE attributes_rowid_tab IS TABLE OF ROWID;

    t_attributes        attributes_tab;
    t_attributes_rowid  attributes_rowid_tab;
    t_triggers          triggers_tab;
    v_context           rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_ATTRIBUTES';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'ATTRIBUTES', t_triggers => t_triggers);

    OPEN attributes_cur;

    OPEN attributes_rowid_cur;

    LOOP
      FETCH attributes_cur
      BULK COLLECT INTO t_attributes LIMIT bulk_limit;

      EXIT WHEN t_attributes.COUNT = 0;

      FETCH attributes_rowid_cur
      BULK COLLECT INTO t_attributes_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_attributes.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT name_type
                             ,name_id
                             ,attribute_type
                             ,ATTRIBUTE
                             ,effective_date
                             ,expiration_date
                             ,state
                             ,partnership_security_label
                             ,status
                             ,comments
                             ,validated_by
                             ,validation_date
                             ,user_id
                             ,attribute1
                             ,attribute2
                             ,attribute3
                             ,attribute4
                             ,attribute5
                             ,attribute6
                             ,attribute7
                             ,attribute8
                             ,attribute9
                             ,attribute10
                             ,attribute11
                             ,attribute12
                             ,attribute13
                             ,attribute14
                             ,attribute15
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,updated_date
                         FROM thot.ATTRIBUTES)
               VALUES t_attributes(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.ATTRIBUTES'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_attributes.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT name_type
                                   ,name_id
                                   ,attribute_type
                                   ,ATTRIBUTE
                                   ,effective_date
                                   ,expiration_date
                                   ,state
                                   ,partnership_security_label
                                   ,status
                                   ,comments
                                   ,validated_by
                                   ,validation_date
                                   ,user_id
                                   ,attribute1
                                   ,attribute2
                                   ,attribute3
                                   ,attribute4
                                   ,attribute5
                                   ,attribute6
                                   ,attribute7
                                   ,attribute8
                                   ,attribute9
                                   ,attribute10
                                   ,attribute11
                                   ,attribute12
                                   ,attribute13
                                   ,attribute14
                                   ,attribute15
                                   ,created_by
                                   ,creation_date
                                   ,updated_by
                                   ,updated_date
                               FROM thot.ATTRIBUTES)
                     VALUES t_attributes(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'NAME_TYPE|NAME_ID'
                                                               ,p_attr1_value   => t_attributes(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).name_type || '|' || t_attributes(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).name_id
                                                               ,p_attr2_name    => 'ATTRIBUTE_TYPE|STATE'
                                                               ,p_attr2_value   => t_attributes(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).attribute_type || '|' || t_attributes(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).state
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_attributes_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_attributes.COUNT || ' | Records inserted:' ||(t_attributes.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_attributes.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.ATTRIBUTES@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_attributes_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE attributes_cur;

    CLOSE attributes_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF attributes_cur%ISOPEN THEN
        CLOSE attributes_cur;
      END IF;

      IF attributes_rowid_cur%ISOPEN THEN
        CLOSE attributes_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_attributes;

--==============================================
--| PULL_PHYSICIAN_DEA: Pulls PHYSICIAN_DEA table to rxhome.  |
--==============================================
  PROCEDURE pull_physician_dea
  IS
    CURSOR physician_dea_cur
    IS
      SELECT   dea_number
              ,expired_flag
              ,business_activity_code
              ,schedule1
              ,schedule2
              ,schedule2n
              ,schedule3
              ,schedule3n
              ,schedule4
              ,schedule5
              ,expiration_date
              ,address1
              ,address2
              ,address3
              ,city
              ,state
              ,zip_code
              ,NAME
              ,activity_sub_code
              ,schedulel1
              ,payment_indicator
          FROM rxh_stage.physician_dea@rxh_stg att
         WHERE att.processed_flag = 'N'
           AND att.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.physician_dea rxh
                           WHERE rxh.dea_number = att.dea_number)
      ORDER BY ROWID;

    CURSOR physician_dea_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.physician_dea@rxh_stg cct
         WHERE cct.processed_flag = 'N'
           AND cct.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.physician_dea rxh
                           WHERE rxh.dea_number = cct.dea_number)
      ORDER BY ROWID;

    TYPE physician_dea_tab IS TABLE OF physician_dea_cur%ROWTYPE;

    TYPE physician_dea_rowid_tab IS TABLE OF ROWID;

    t_physician_dea        physician_dea_tab;
    t_physician_dea_rowid  physician_dea_rowid_tab;
    t_triggers             triggers_tab;
    v_context              rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PHYSICIAN_DEA';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PHYSICIAN_DEA', t_triggers => t_triggers);

    OPEN physician_dea_cur;

    OPEN physician_dea_rowid_cur;

    LOOP
      FETCH physician_dea_cur
      BULK COLLECT INTO t_physician_dea LIMIT bulk_limit;

      EXIT WHEN t_physician_dea.COUNT = 0;

      FETCH physician_dea_rowid_cur
      BULK COLLECT INTO t_physician_dea_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_physician_dea.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT dea_number
                             ,expired_flag
                             ,business_activity_code
                             ,schedule1
                             ,schedule2
                             ,schedule2n
                             ,schedule3
                             ,schedule3n
                             ,schedule4
                             ,schedule5
                             ,expiration_date
                             ,address1
                             ,address2
                             ,address3
                             ,city
                             ,state
                             ,zip_code
                             ,NAME
                             ,activity_sub_code
                             ,schedulel1
                             ,payment_indicator
                         FROM thot.physician_dea)
               VALUES t_physician_dea(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PHYSICIAN_DEA'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_physician_dea.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT dea_number
                                   ,expired_flag
                                   ,business_activity_code
                                   ,schedule1
                                   ,schedule2
                                   ,schedule2n
                                   ,schedule3
                                   ,schedule3n
                                   ,schedule4
                                   ,schedule5
                                   ,expiration_date
                                   ,address1
                                   ,address2
                                   ,address3
                                   ,city
                                   ,state
                                   ,zip_code
                                   ,NAME
                                   ,activity_sub_code
                                   ,schedulel1
                                   ,payment_indicator
                               FROM thot.physician_dea)
                     VALUES t_physician_dea(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'DEA_NUMBER'
                                                               ,p_attr1_value   => t_physician_dea(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).dea_number
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_physician_dea_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_physician_dea.COUNT || ' | Records inserted:' ||(t_physician_dea.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_physician_dea.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.physician_dea@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_physician_dea_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE physician_dea_cur;

    CLOSE physician_dea_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF physician_dea_cur%ISOPEN THEN
        CLOSE physician_dea_cur;
      END IF;

      IF physician_dea_rowid_cur%ISOPEN THEN
        CLOSE physician_dea_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_physician_dea;

--=============================================+
--| PULL_NPI_TABLE: Pulls NPI_TABLE table to rxhome.                        |
--=============================================+
  PROCEDURE pull_npi_table
  IS
    CURSOR npi_table_cur
    IS
      SELECT   npi_id
              ,NAME
              ,expire_date
              ,expired_flag
              ,address
              ,city
              ,state
              ,zip_code
              ,creation_date
              ,created_by
              ,update_date
              ,updated_by
              ,entity_type_code
          FROM rxh_stage.npi_table@rxh_stg npi
         WHERE npi.processed_flag = 'N'
           AND npi.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.npi_table rxh
                           WHERE rxh.npi_id = npi.npi_id)
      ORDER BY ROWID;

    CURSOR npi_table_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.npi_table@rxh_stg npi
         WHERE npi.processed_flag = 'N'
           AND npi.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.npi_table rxh
                           WHERE rxh.npi_id = npi.npi_id)
      ORDER BY ROWID;

    TYPE npi_table_tab IS TABLE OF npi_table_cur%ROWTYPE;

    TYPE npi_table_rowid_tab IS TABLE OF ROWID;

    t_npi_table        npi_table_tab;
    t_npi_table_rowid  npi_table_rowid_tab;
    t_triggers         triggers_tab;
    v_context          rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_NPI_TABLE';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'NPI_TABLE', t_triggers => t_triggers);

    OPEN npi_table_cur;

    OPEN npi_table_rowid_cur;

    LOOP
      FETCH npi_table_cur
      BULK COLLECT INTO t_npi_table LIMIT bulk_limit;

      EXIT WHEN t_npi_table.COUNT = 0;

      FETCH npi_table_rowid_cur
      BULK COLLECT INTO t_npi_table_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_npi_table.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT npi_id
                             ,NAME
                             ,expire_date
                             ,expired_flag
                             ,address
                             ,city
                             ,state
                             ,zip_code
                             ,creation_date
                             ,created_by
                             ,update_date
                             ,updated_by
                             ,entity_type_code
                         FROM thot.npi_table)
               VALUES t_npi_table(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.NPI_TABLE'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_npi_table.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT npi_id
                                   ,NAME
                                   ,expire_date
                                   ,expired_flag
                                   ,address
                                   ,city
                                   ,state
                                   ,zip_code
                                   ,creation_date
                                   ,created_by
                                   ,update_date
                                   ,updated_by
                                   ,entity_type_code
                               FROM thot.npi_table)
                     VALUES t_npi_table(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'NPI_ID'
                                                               ,p_attr1_value   => t_npi_table(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).npi_id
                                                               ,p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_npi_table_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_npi_table.COUNT || ' | Records inserted:' ||(t_npi_table.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_npi_table.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.npi_table@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_npi_table_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE npi_table_cur;

    CLOSE npi_table_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF npi_table_cur%ISOPEN THEN
        CLOSE npi_table_cur;
      END IF;

      IF npi_table_rowid_cur%ISOPEN THEN
        CLOSE npi_table_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_npi_table;

--==========================================================
--| PULL_PRESCRIPTIONS_TABLE: Pulls PRESCRIPTIONS_TABLE table to rxhome.      |
--==========================================================
  PROCEDURE pull_prescriptions_table
  IS
    CURSOR prescriptions_table_cur
    IS
      SELECT   svcbr_id
              ,prescription_id
              ,refill_no
              ,patient_id
              ,cmp_id
              ,therapy_id
              ,therapy_type
              ,drug_route
              ,max_refills
              ,svcbr_copied_from
              ,presc_copied_from
              ,refill_copied_from
              ,therapy_start_date
              ,shipment_id
              ,prescr_ship_date
              ,no_days_shipment_last
              ,no_doses
              ,frequency
              ,rate
              ,rate_unit
              ,rate_time
              ,infusion_period_min
              ,infusion_period_hr
              ,infusion_period_day
              ,physician_id
              ,verbal_order
              ,pharmacist_initials
              ,technician_initials
              ,no_labels_print
              ,expiration_date
              ,storage_type
              ,next_delivery_date
              ,next_delivery_note
              ,special_instruct
              ,label_instruct
              ,delivery_instruct
              ,times_printed
              ,rx_printok
              ,dvr_printok
              ,label_printok
              ,completed_date
              ,completed_xuser
              ,xuser
              ,void_date
              ,void_reason
              ,bolus
              ,bolus_unit
              ,bolus_time
              ,total_volume
              ,rxnc
              ,rxlock
              ,pos
              ,tos
              ,employment_flag
              ,auto_accident_flag
              ,other_accident_flag
              ,accident_state
              ,rx_written_date
              ,rx_start_date
              ,rx_stop_date
              ,refill_thru_date
              ,it_sb_loc_type
              ,it_sb_loc_id
              ,ancillary_prescriptions_count
              ,compounding_detail_count
              ,noncompounded_detail_count
              ,caregiver_detail_count
              ,rx_dme_count
              ,rxauth_count
              ,rxcmn_count
              ,rx_description
              ,rx_checked_flag
              ,ca_checked_flag
              ,dc_checked_flag
              ,di_checked_flag
              ,dz_checked_flag
              ,pe_checked_flag
              ,pm_checked_flag
              ,pr_checked_flag
              ,td_checked_flag
              ,dur_response_flag
              ,rx_status
              ,rx_status_date
              ,rx_status_comment
              ,triplicate_serial_no
              ,occurrence_date
              ,completed_flag
              ,refill_flag
              ,refill_xuser
              ,refill_xdate
              ,rort
              ,rort_xuser
              ,rort_date
              ,patient_addr_seq
              ,exhaust_date
              ,md_sign_xuser
              ,md_sign_xdate
              ,copy_xfer_flag
              ,shipping_id
              ,daw_code
              ,partnership_security_label
              ,order_taken_code
              ,shipping_rule_no
              ,profile_lock_initials
              ,creation_date
              ,created_by
              ,update_date
              ,updated_by
              ,auto_rightlock_flag
              ,saturday_delivery
              ,profile_lock_date
              ,override_reason
              ,override_xuser
              ,override_xdate
              ,override_type
              ,ny_medicaid_serial_no
              ,pac_rx
              ,pac_reason_code
              ,pac_rx_status
              ,pac_interface_flag
              ,auto_srs_flag
              ,pac_qa_hit_code
              ,pac_qa_hit_comment
              ,phys_addr_seq
              ,renewal_autofax
              ,ns_ord_order_date
              ,ship_rsn_id
              ,taper_up
              ,taper_down
              ,vol_per_dispensed_unit
              ,batch_volume
              ,fixed_volume
              ,overfill_percentage
              ,batch_vol_overfill
              ,compounding_instructions
              ,rx_dtk_indicator_count
              ,rx_dtk_patient_weigth_kg
              ,rx_dtk_patient_weigth_lb
              ,oncology_auth_number
              ,oncology_auth_exp_date
              ,pah_dosing_weight
              ,pah_dose
              ,pah_dose_unit
              ,var_fill_indicator
              ,var_fill_rampup_flag
              ,var_fill_dec_reason
              ,max_dispensed_qty
              ,max_rx_lt_qty
              ,max_presc_rx_lt_qty
              ,qty_this_fill
              ,qty_next_fill
              ,rem_rx_lt_qty
              ,possible_svcbr_id_1
              ,possible_svcbr_id_2
              ,possible_svcbr_id_3
              ,possible_svcbr_id_4
              ,possible_svcbr_id_5
              ,est_delivery_date
              ,confirmation_nbr
          FROM rxh_stage.prescriptions_table@rxh_stg rx
         WHERE rx.processed_flag = 'N'
           AND rx.ready_flag = 'Y'
           AND rx.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = rx.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.prescriptions_table rxh
                           WHERE rxh.svcbr_id = rx.svcbr_id
                             AND rxh.prescription_id = rx.prescription_id
                             AND rxh.refill_no = rx.refill_no)
      ORDER BY ROWID;

    CURSOR prescriptions_table_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.prescriptions_table@rxh_stg rx
         WHERE rx.processed_flag = 'N'
           AND rx.ready_flag = 'Y'
           AND rx.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = rx.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.prescriptions_table rxh
                           WHERE rxh.svcbr_id = rx.svcbr_id
                             AND rxh.prescription_id = rx.prescription_id
                             AND rxh.refill_no = rx.refill_no)
      ORDER BY ROWID;

    TYPE prescriptions_table_tab IS TABLE OF prescriptions_table_cur%ROWTYPE;

    TYPE prescriptions_table_rowid_tab IS TABLE OF ROWID;

    t_prescriptions_table        prescriptions_table_tab;
    t_prescriptions_table_rowid  prescriptions_table_rowid_tab;
    t_triggers                   triggers_tab;
    v_context                    rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PRESCRIPTIONS_TABLE';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PRESCRIPTIONS_TABLE', t_triggers => t_triggers);

    OPEN prescriptions_table_cur;

    OPEN prescriptions_table_rowid_cur;

    LOOP
      FETCH prescriptions_table_cur
      BULK COLLECT INTO t_prescriptions_table LIMIT bulk_limit;

      EXIT WHEN t_prescriptions_table.COUNT = 0;

      FETCH prescriptions_table_rowid_cur
      BULK COLLECT INTO t_prescriptions_table_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_prescriptions_table.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT svcbr_id
                             ,prescription_id
                             ,refill_no
                             ,patient_id
                             ,cmp_id
                             ,therapy_id
                             ,therapy_type
                             ,drug_route
                             ,max_refills
                             ,svcbr_copied_from
                             ,presc_copied_from
                             ,refill_copied_from
                             ,therapy_start_date
                             ,shipment_id
                             ,prescr_ship_date
                             ,no_days_shipment_last
                             ,no_doses
                             ,frequency
                             ,rate
                             ,rate_unit
                             ,rate_time
                             ,infusion_period_min
                             ,infusion_period_hr
                             ,infusion_period_day
                             ,physician_id
                             ,verbal_order
                             ,pharmacist_initials
                             ,technician_initials
                             ,no_labels_print
                             ,expiration_date
                             ,storage_type
                             ,next_delivery_date
                             ,next_delivery_note
                             ,special_instruct
                             ,label_instruct
                             ,delivery_instruct
                             ,times_printed
                             ,rx_printok
                             ,dvr_printok
                             ,label_printok
                             ,completed_date
                             ,completed_xuser
                             ,xuser
                             ,void_date
                             ,void_reason
                             ,bolus
                             ,bolus_unit
                             ,bolus_time
                             ,total_volume
                             ,rxnc
                             ,rxlock
                             ,pos
                             ,tos
                             ,employment_flag
                             ,auto_accident_flag
                             ,other_accident_flag
                             ,accident_state
                             ,rx_written_date
                             ,rx_start_date
                             ,rx_stop_date
                             ,refill_thru_date
                             ,it_sb_loc_type
                             ,it_sb_loc_id
                             ,ancillary_prescriptions_count
                             ,compounding_detail_count
                             ,noncompounded_detail_count
                             ,caregiver_detail_count
                             ,rx_dme_count
                             ,rxauth_count
                             ,rxcmn_count
                             ,rx_description
                             ,rx_checked_flag
                             ,ca_checked_flag
                             ,dc_checked_flag
                             ,di_checked_flag
                             ,dz_checked_flag
                             ,pe_checked_flag
                             ,pm_checked_flag
                             ,pr_checked_flag
                             ,td_checked_flag
                             ,dur_response_flag
                             ,rx_status
                             ,rx_status_date
                             ,rx_status_comment
                             ,triplicate_serial_no
                             ,occurrence_date
                             ,completed_flag
                             ,refill_flag
                             ,refill_xuser
                             ,refill_xdate
                             ,rort
                             ,rort_xuser
                             ,rort_date
                             ,patient_addr_seq
                             ,exhaust_date
                             ,md_sign_xuser
                             ,md_sign_xdate
                             ,copy_xfer_flag
                             ,shipping_id
                             ,daw_code
                             ,partnership_security_label
                             ,order_taken_code
                             ,shipping_rule_no
                             ,profile_lock_initials
                             ,creation_date
                             ,created_by
                             ,update_date
                             ,updated_by
                             ,auto_rightlock_flag
                             ,saturday_delivery
                             ,profile_lock_date
                             ,override_reason
                             ,override_xuser
                             ,override_xdate
                             ,override_type
                             ,ny_medicaid_serial_no
                             ,pac_rx
                             ,pac_reason_code
                             ,pac_rx_status
                             ,pac_interface_flag
                             ,auto_srs_flag
                             ,pac_qa_hit_code
                             ,pac_qa_hit_comment
                             ,phys_addr_seq
                             ,renewal_autofax
                             ,ns_ord_order_date
                             ,ship_rsn_id
                             ,taper_up
                             ,taper_down
                             ,vol_per_dispensed_unit
                             ,batch_volume
                             ,fixed_volume
                             ,overfill_percentage
                             ,batch_vol_overfill
                             ,compounding_instructions
                             ,rx_dtk_indicator_count
                             ,rx_dtk_patient_weigth_kg
                             ,rx_dtk_patient_weigth_lb
                             ,oncology_auth_number
                             ,oncology_auth_exp_date
                             ,pah_dosing_weight
                             ,pah_dose
                             ,pah_dose_unit
                             ,var_fill_indicator
                             ,var_fill_rampup_flag
                             ,var_fill_dec_reason
                             ,max_dispensed_qty
                             ,max_rx_lt_qty
                             ,max_presc_rx_lt_qty
                             ,qty_this_fill
                             ,qty_next_fill
                             ,rem_rx_lt_qty
                             ,possible_svcbr_id_1
                             ,possible_svcbr_id_2
                             ,possible_svcbr_id_3
                             ,possible_svcbr_id_4
                             ,possible_svcbr_id_5
                             ,est_delivery_date
                             ,confirmation_nbr
                         FROM thot.prescriptions_table)
               VALUES t_prescriptions_table(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PRESCRIPTIONS_TABLE'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_prescriptions_table.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT svcbr_id
                                   ,prescription_id
                                   ,refill_no
                                   ,patient_id
                                   ,cmp_id
                                   ,therapy_id
                                   ,therapy_type
                                   ,drug_route
                                   ,max_refills
                                   ,svcbr_copied_from
                                   ,presc_copied_from
                                   ,refill_copied_from
                                   ,therapy_start_date
                                   ,shipment_id
                                   ,prescr_ship_date
                                   ,no_days_shipment_last
                                   ,no_doses
                                   ,frequency
                                   ,rate
                                   ,rate_unit
                                   ,rate_time
                                   ,infusion_period_min
                                   ,infusion_period_hr
                                   ,infusion_period_day
                                   ,physician_id
                                   ,verbal_order
                                   ,pharmacist_initials
                                   ,technician_initials
                                   ,no_labels_print
                                   ,expiration_date
                                   ,storage_type
                                   ,next_delivery_date
                                   ,next_delivery_note
                                   ,special_instruct
                                   ,label_instruct
                                   ,delivery_instruct
                                   ,times_printed
                                   ,rx_printok
                                   ,dvr_printok
                                   ,label_printok
                                   ,completed_date
                                   ,completed_xuser
                                   ,xuser
                                   ,void_date
                                   ,void_reason
                                   ,bolus
                                   ,bolus_unit
                                   ,bolus_time
                                   ,total_volume
                                   ,rxnc
                                   ,rxlock
                                   ,pos
                                   ,tos
                                   ,employment_flag
                                   ,auto_accident_flag
                                   ,other_accident_flag
                                   ,accident_state
                                   ,rx_written_date
                                   ,rx_start_date
                                   ,rx_stop_date
                                   ,refill_thru_date
                                   ,it_sb_loc_type
                                   ,it_sb_loc_id
                                   ,ancillary_prescriptions_count
                                   ,compounding_detail_count
                                   ,noncompounded_detail_count
                                   ,caregiver_detail_count
                                   ,rx_dme_count
                                   ,rxauth_count
                                   ,rxcmn_count
                                   ,rx_description
                                   ,rx_checked_flag
                                   ,ca_checked_flag
                                   ,dc_checked_flag
                                   ,di_checked_flag
                                   ,dz_checked_flag
                                   ,pe_checked_flag
                                   ,pm_checked_flag
                                   ,pr_checked_flag
                                   ,td_checked_flag
                                   ,dur_response_flag
                                   ,rx_status
                                   ,rx_status_date
                                   ,rx_status_comment
                                   ,triplicate_serial_no
                                   ,occurrence_date
                                   ,completed_flag
                                   ,refill_flag
                                   ,refill_xuser
                                   ,refill_xdate
                                   ,rort
                                   ,rort_xuser
                                   ,rort_date
                                   ,patient_addr_seq
                                   ,exhaust_date
                                   ,md_sign_xuser
                                   ,md_sign_xdate
                                   ,copy_xfer_flag
                                   ,shipping_id
                                   ,daw_code
                                   ,partnership_security_label
                                   ,order_taken_code
                                   ,shipping_rule_no
                                   ,profile_lock_initials
                                   ,creation_date
                                   ,created_by
                                   ,update_date
                                   ,updated_by
                                   ,auto_rightlock_flag
                                   ,saturday_delivery
                                   ,profile_lock_date
                                   ,override_reason
                                   ,override_xuser
                                   ,override_xdate
                                   ,override_type
                                   ,ny_medicaid_serial_no
                                   ,pac_rx
                                   ,pac_reason_code
                                   ,pac_rx_status
                                   ,pac_interface_flag
                                   ,auto_srs_flag
                                   ,pac_qa_hit_code
                                   ,pac_qa_hit_comment
                                   ,phys_addr_seq
                                   ,renewal_autofax
                                   ,ns_ord_order_date
                                   ,ship_rsn_id
                                   ,taper_up
                                   ,taper_down
                                   ,vol_per_dispensed_unit
                                   ,batch_volume
                                   ,fixed_volume
                                   ,overfill_percentage
                                   ,batch_vol_overfill
                                   ,compounding_instructions
                                   ,rx_dtk_indicator_count
                                   ,rx_dtk_patient_weigth_kg
                                   ,rx_dtk_patient_weigth_lb
                                   ,oncology_auth_number
                                   ,oncology_auth_exp_date
                                   ,pah_dosing_weight
                                   ,pah_dose
                                   ,pah_dose_unit
                                   ,var_fill_indicator
                                   ,var_fill_rampup_flag
                                   ,var_fill_dec_reason
                                   ,max_dispensed_qty
                                   ,max_rx_lt_qty
                                   ,max_presc_rx_lt_qty
                                   ,qty_this_fill
                                   ,qty_next_fill
                                   ,rem_rx_lt_qty
                                   ,possible_svcbr_id_1
                                   ,possible_svcbr_id_2
                                   ,possible_svcbr_id_3
                                   ,possible_svcbr_id_4
                                   ,possible_svcbr_id_5
                                   ,est_delivery_date
                                   ,confirmation_nbr
                               FROM thot.prescriptions_table)
                     VALUES t_prescriptions_table(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'SVCBR_ID'
                                                               ,p_attr1_value   => t_prescriptions_table(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).svcbr_id
                                                               ,p_attr2_name    => 'PRESCRIPTION_ID'
                                                               ,p_attr2_value   => t_prescriptions_table(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).prescription_id
                                                               ,p_attr3_name    => 'REFILL_NO'
                                                               ,p_attr3_value   => t_prescriptions_table(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).refill_no
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_prescriptions_table_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_prescriptions_table.COUNT || ' | Records inserted:' ||(t_prescriptions_table.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_prescriptions_table.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.prescriptions_table@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_prescriptions_table_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE prescriptions_table_cur;

    CLOSE prescriptions_table_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF prescriptions_table_cur%ISOPEN THEN
        CLOSE prescriptions_table_cur;
      END IF;

      IF prescriptions_table_rowid_cur%ISOPEN THEN
        CLOSE prescriptions_table_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_prescriptions_table;

--=========================================================+
--| PULL_PRESCRIPTION_DRUGS: Pulls PRESCRIPTION_DRUGS table to rxhome.               |
--=========================================================+
  PROCEDURE pull_prescription_drugs
  IS
    CURSOR prescription_drugs_cur
    IS
      SELECT   svcbr_id
              ,prescription_id
              ,refill_no
              ,drug_abbrev
              ,dosage
              ,dosage_unit
              ,dose_per
              ,unit_per
              ,recalc_flag
              ,partnership_security_label
              ,drug_prescribed
              ,creation_date
              ,created_by
              ,update_date
              ,updated_by
              ,line_id
              ,range_from
              ,range_to
              ,range_units
              ,no_doses
              ,frequency
              ,no_labels_print
              ,drug_prescribed_inv_id
              ,overfill_percentage
              ,overfill_dosage
              ,total_dosage
              ,total_overfill_dosage
              ,rx_dtk_prescribed_dose
              ,rx_dtk_prescribed_dose_unit
              ,rx_dtk_prescribed_weight_unit
              ,rx_dtk_calculated_dose_kg
              ,rx_dtk_calculated_dose_lb
              ,rx_dtk_flag
          FROM rxh_stage.prescription_drugs@rxh_stg ptd
         WHERE ptd.processed_flag = 'N'
           AND ptd.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = ptd.svcbr_id
                         AND rxh.prescription_id = ptd.prescription_id
                         AND rxh.refill_no = ptd.refill_no)
      ORDER BY ROWID;

    CURSOR prescription_drugs_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.prescription_drugs@rxh_stg ptd
         WHERE ptd.processed_flag = 'N'
           AND ptd.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = ptd.svcbr_id
                         AND rxh.prescription_id = ptd.prescription_id
                         AND rxh.refill_no = ptd.refill_no)
      ORDER BY ROWID;

    TYPE prescription_drugs_tab IS TABLE OF prescription_drugs_cur%ROWTYPE;

    TYPE prescription_drugs_rowid_tab IS TABLE OF ROWID;

    t_prescription_drugs        prescription_drugs_tab;
    t_prescription_drugs_rowid  prescription_drugs_rowid_tab;
    t_triggers                  triggers_tab;
    v_context                   rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PRESCRIPTION_DRUGS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PRESCRIPTION_DRUGS', t_triggers => t_triggers);

    OPEN prescription_drugs_cur;

    OPEN prescription_drugs_rowid_cur;

    LOOP
      FETCH prescription_drugs_cur
      BULK COLLECT INTO t_prescription_drugs LIMIT bulk_limit;

      EXIT WHEN t_prescription_drugs.COUNT = 0;

      FETCH prescription_drugs_rowid_cur
      BULK COLLECT INTO t_prescription_drugs_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_prescription_drugs.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT svcbr_id
                             ,prescription_id
                             ,refill_no
                             ,drug_abbrev
                             ,dosage
                             ,dosage_unit
                             ,dose_per
                             ,unit_per
                             ,recalc_flag
                             ,partnership_security_label
                             ,drug_prescribed
                             ,creation_date
                             ,created_by
                             ,update_date
                             ,updated_by
                             ,line_id
                             ,range_from
                             ,range_to
                             ,range_units
                             ,no_doses
                             ,frequency
                             ,no_labels_print
                             ,drug_prescribed_inv_id
                             ,overfill_percentage
                             ,overfill_dosage
                             ,total_dosage
                             ,total_overfill_dosage
                             ,rx_dtk_prescribed_dose
                             ,rx_dtk_prescribed_dose_unit
                             ,rx_dtk_prescribed_weight_unit
                             ,rx_dtk_calculated_dose_kg
                             ,rx_dtk_calculated_dose_lb
                             ,rx_dtk_flag
                         FROM thot.prescription_drugs)
               VALUES t_prescription_drugs(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PRESCRIPTION_DRUGS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_prescription_drugs.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT svcbr_id
                                   ,prescription_id
                                   ,refill_no
                                   ,drug_abbrev
                                   ,dosage
                                   ,dosage_unit
                                   ,dose_per
                                   ,unit_per
                                   ,recalc_flag
                                   ,partnership_security_label
                                   ,drug_prescribed
                                   ,creation_date
                                   ,created_by
                                   ,update_date
                                   ,updated_by
                                   ,line_id
                                   ,range_from
                                   ,range_to
                                   ,range_units
                                   ,no_doses
                                   ,frequency
                                   ,no_labels_print
                                   ,drug_prescribed_inv_id
                                   ,overfill_percentage
                                   ,overfill_dosage
                                   ,total_dosage
                                   ,total_overfill_dosage
                                   ,rx_dtk_prescribed_dose
                                   ,rx_dtk_prescribed_dose_unit
                                   ,rx_dtk_prescribed_weight_unit
                                   ,rx_dtk_calculated_dose_kg
                                   ,rx_dtk_calculated_dose_lb
                                   ,rx_dtk_flag
                               FROM thot.prescription_drugs)
                     VALUES t_prescription_drugs(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'SVCBR_ID|PRESCRIPTION_ID|REFILL_NO'
                                                               ,p_attr1_value   =>    t_prescription_drugs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).svcbr_id
                                                                                   || '|'
                                                                                   || t_prescription_drugs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).prescription_id
                                                                                   || '|'
                                                                                   || t_prescription_drugs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).refill_no
                                                               ,p_attr2_name    => 'DOSAGE_UNIT'
                                                               ,p_attr2_value   => t_prescription_drugs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).dosage_unit
                                                               ,p_attr3_name    => 'UNIT_PER'
                                                               ,p_attr3_value   => t_prescription_drugs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).unit_per
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_prescription_drugs_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_prescription_drugs.COUNT || ' | Records inserted:' ||(t_prescription_drugs.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_prescription_drugs.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.prescription_drugs@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_prescription_drugs_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE prescription_drugs_cur;

    CLOSE prescription_drugs_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF prescription_drugs_cur%ISOPEN THEN
        CLOSE prescription_drugs_cur;
      END IF;

      IF prescription_drugs_rowid_cur%ISOPEN THEN
        CLOSE prescription_drugs_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_prescription_drugs;

--=============================================================
--| PULL_NONCOMPOUNDED_DETAIL: Pulls NONCOMPOUNDED_DETAIL table to rxhome.          |
--=============================================================
  PROCEDURE pull_noncompounded_detail
  IS
    CURSOR noncompounded_detail_cur
    IS
      SELECT   svcbr_id
              ,prescription_id
              ,refill_no
              ,inventory_id
              ,quantity_per
              ,no_days
              ,quantity_to_ship
              ,ob
              ,lot
              ,no_doses
              ,partnership_security_label
              ,creation_date
              ,created_by
              ,update_date
              ,updated_by
              ,line_id
              ,rxsub_id
              ,dm_flag
          FROM rxh_stage.noncompounded_detail@rxh_stg non
         WHERE non.processed_flag = 'N'
           AND non.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = non.svcbr_id
                         AND rxh.prescription_id = non.prescription_id
                         AND rxh.refill_no = non.refill_no)
           AND NOT EXISTS(SELECT 1
                            FROM thot.noncompounded_detail rxh
                           WHERE rxh.svcbr_id = non.svcbr_id
                             AND rxh.prescription_id = non.prescription_id
                             AND rxh.refill_no = non.refill_no
                             AND rxh.line_id = non.line_id
                             AND rxh.inventory_id = non.inventory_id
                             AND rxh.lot = non.lot)
      ORDER BY ROWID;

    CURSOR noncompounded_detail_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.noncompounded_detail@rxh_stg non
         WHERE non.processed_flag = 'N'
           AND non.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = non.svcbr_id
                         AND rxh.prescription_id = non.prescription_id
                         AND rxh.refill_no = non.refill_no)
           AND NOT EXISTS(SELECT 1
                            FROM thot.noncompounded_detail rxh
                           WHERE rxh.svcbr_id = non.svcbr_id
                             AND rxh.prescription_id = non.prescription_id
                             AND rxh.refill_no = non.refill_no
                             AND rxh.line_id = non.line_id
                             AND rxh.inventory_id = non.inventory_id
                             AND rxh.lot = non.lot)
      ORDER BY ROWID;

    TYPE noncompounded_detail_tab IS TABLE OF noncompounded_detail_cur%ROWTYPE;

    TYPE noncompounded_detail_rowid_tab IS TABLE OF ROWID;

    t_noncompounded_detail        noncompounded_detail_tab;
    t_noncompounded_detail_rowid  noncompounded_detail_rowid_tab;
    t_triggers                    triggers_tab;
    v_context                     rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_NONCOMPOUNDED_DETAIL';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'NONCOMPOUNDED_DETAIL', t_triggers => t_triggers);

    OPEN noncompounded_detail_cur;

    OPEN noncompounded_detail_rowid_cur;

    LOOP
      FETCH noncompounded_detail_cur
      BULK COLLECT INTO t_noncompounded_detail LIMIT bulk_limit;

      EXIT WHEN t_noncompounded_detail.COUNT = 0;

      FETCH noncompounded_detail_rowid_cur
      BULK COLLECT INTO t_noncompounded_detail_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_noncompounded_detail.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT svcbr_id
                             ,prescription_id
                             ,refill_no
                             ,inventory_id
                             ,quantity_per
                             ,no_days
                             ,quantity_to_ship
                             ,ob
                             ,lot
                             ,no_doses
                             ,partnership_security_label
                             ,creation_date
                             ,created_by
                             ,update_date
                             ,updated_by
                             ,line_id
                             ,rxsub_id
                             ,dm_flag
                         FROM thot.noncompounded_detail)
               VALUES t_noncompounded_detail(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.NONCOMPOUNDED_DETAIL'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_noncompounded_detail.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT svcbr_id
                                   ,prescription_id
                                   ,refill_no
                                   ,inventory_id
                                   ,quantity_per
                                   ,no_days
                                   ,quantity_to_ship
                                   ,ob
                                   ,lot
                                   ,no_doses
                                   ,partnership_security_label
                                   ,creation_date
                                   ,created_by
                                   ,update_date
                                   ,updated_by
                                   ,line_id
                                   ,rxsub_id
                                   ,dm_flag
                               FROM thot.noncompounded_detail)
                     VALUES t_noncompounded_detail(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'SVCBR_ID|PRESCRIPTION_ID|REFILL_NO'
                                                               ,p_attr1_value   =>    t_noncompounded_detail(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).svcbr_id
                                                                                   || '|'
                                                                                   || t_noncompounded_detail(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).prescription_id
                                                                                   || '|'
                                                                                   || t_noncompounded_detail(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).refill_no
                                                               ,p_attr2_name    => 'INVENTORY_ID'
                                                               ,p_attr2_value   => t_noncompounded_detail(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).inventory_id
                                                               ,p_attr3_name    => 'LOT'
                                                               ,p_attr3_value   => t_noncompounded_detail(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).lot
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_noncompounded_detail_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_noncompounded_detail.COUNT || ' | Records inserted:' ||(t_noncompounded_detail.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_noncompounded_detail.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.noncompounded_detail@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_noncompounded_detail_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE noncompounded_detail_cur;

    CLOSE noncompounded_detail_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF noncompounded_detail_cur%ISOPEN THEN
        CLOSE noncompounded_detail_cur;
      END IF;

      IF noncompounded_detail_rowid_cur%ISOPEN THEN
        CLOSE noncompounded_detail_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_noncompounded_detail;

--=============================================================
--| PULL_COMPOUNDING_RECORD: Pulls COMPOUNDING_RECORD table to rxhome.          |
--=============================================================
  PROCEDURE pull_compounding_record
  IS
    CURSOR compounding_record_cur
    IS
        SELECT svcbr_id
              ,prescription_id
              ,refill_no
              ,compounded_date
              ,quantity_used
              ,comp_rx_printok
              ,partnership_security_label
              ,creation_date
              ,created_by
              ,update_date
              ,updated_by
        FROM rxh_stage.compounding_record@rxh_stg non
       WHERE non.processed_flag = 'N'
         AND non.run_id = g_run_id
         AND EXISTS(SELECT 1
                      FROM thot.prescriptions_table rxh
                     WHERE rxh.svcbr_id            = non.svcbr_id
                       AND rxh.prescription_id     = non.prescription_id
                       AND rxh.refill_no           = non.refill_no)
         AND NOT EXISTS(SELECT 1
                          FROM thot.compounding_record rxh
                         WHERE rxh.svcbr_id        = non.svcbr_id
                           AND rxh.prescription_id = non.prescription_id
                           AND rxh.refill_no       = non.refill_no)
    ORDER BY ROWID;

    CURSOR compounding_rec_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.compounding_record@rxh_stg non
         WHERE non.processed_flag = 'N'
           AND non.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id        = non.svcbr_id
                         AND rxh.prescription_id = non.prescription_id
                         AND rxh.refill_no       = non.refill_no
                     )
           AND NOT EXISTS(SELECT 1
                            FROM thot.compounding_detail rxh
                           WHERE rxh.svcbr_id        = non.svcbr_id
                             AND rxh.prescription_id = non.prescription_id
                             AND rxh.refill_no       = non.refill_no)
      ORDER BY ROWID;

    TYPE compounding_record_tab IS TABLE OF compounding_record_cur%ROWTYPE;

    TYPE compounding_record_rowid_tab IS TABLE OF ROWID;

    t_compounding_record        compounding_record_tab;
    t_compounding_record_rowid  compounding_record_rowid_tab;
    t_triggers                  triggers_tab;
    v_context                   rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_COMPOUNDING_RECORD';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'COMPOUNDING_RECORD', t_triggers => t_triggers);

    OPEN compounding_record_cur;

    OPEN compounding_rec_rowid_cur;

    LOOP
      FETCH compounding_record_cur
      BULK COLLECT INTO t_compounding_record LIMIT bulk_limit;

      EXIT WHEN t_compounding_record.COUNT = 0;

      FETCH compounding_rec_rowid_cur
      BULK COLLECT INTO t_compounding_record_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_compounding_record.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT svcbr_id
                              ,prescription_id
                              ,refill_no
                              ,compounded_date
                              ,quantity_used
                              ,comp_rx_printok
                              ,partnership_security_label
                              ,creation_date
                              ,created_by
                              ,update_date
                              ,updated_by
                        FROM thot.compounding_record)
               VALUES t_compounding_record(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.COMPOUNDING_RECORD'
                                                             ,p_run_id     => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_compounding_record.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT svcbr_id
                                    ,prescription_id
                                    ,refill_no
                                    ,compounded_date
                                    ,quantity_used
                                    ,comp_rx_printok
                                    ,partnership_security_label
                                    ,creation_date
                                    ,created_by
                                    ,update_date
                                    ,updated_by
                              FROM thot.compounding_record)
                     VALUES t_compounding_record(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'SVCBR_ID|PRESCRIPTION_ID|REFILL_NO'
                                                               ,p_attr1_value   =>    t_compounding_record(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).svcbr_id
                                                                                   || '|'
                                                                                   || t_compounding_record(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).prescription_id
                                                                                   || '|'
                                                                                   || t_compounding_record(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).refill_no
                                                               ,p_run_id     => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_compounding_record_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context   => v_context,
                                              p_error_msg => 'Records processed :' || t_compounding_record.COUNT || ' | Records inserted:' ||(t_compounding_record.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                            , p_run_id => g_run_id);

      FOR i IN 1 .. t_compounding_record.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.compounding_record@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_compounding_record_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE compounding_record_cur;

    CLOSE compounding_rec_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF compounding_record_cur%ISOPEN THEN
        CLOSE compounding_record_cur;
      END IF;

      IF compounding_rec_rowid_cur%ISOPEN THEN
        CLOSE compounding_rec_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_compounding_record;

--=============================================================
--| PULL_COMPOUNDING_DETAIL: Pulls COMPOUNDIG_DETAIL table to rxhome.          |
--=============================================================
  PROCEDURE pull_compounding_detail
  IS
    CURSOR compounding_detail_cur
    IS
        SELECT svcbr_id
             , prescription_id
             , refill_no
             , compounded_date
             , inventory_id
             , quantity_used
             , ob
             , lot
             , quantity_per
             , no_days
             , no_doses
             , partnership_security_label
             , creation_date
             , created_by
             , update_date
             , updated_by
             , line_id
             , rxsub_id
             , dm_flag
             , tpn_drug_id
             , tpn_pool_bag_flag
             , number_of_items
          FROM rxh_stage.compounding_detail@rxh_stg non
         WHERE non.processed_flag = 'N'
           AND non.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id            = non.svcbr_id
                         AND rxh.prescription_id     = non.prescription_id
                         AND rxh.refill_no           = non.refill_no)
           AND NOT EXISTS(SELECT 1
                            FROM thot.compounding_detail rxh
                           WHERE rxh.svcbr_id        = non.svcbr_id
                             AND rxh.prescription_id = non.prescription_id
                             AND rxh.refill_no       = non.refill_no
                             AND rxh.line_id         = non.line_id
                             AND rxh.inventory_id    = non.inventory_id
                             AND rxh.lot             = non.lot
                          )
      ORDER BY ROWID;

    CURSOR compounding_detail_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.compounding_detail@rxh_stg non
         WHERE non.processed_flag = 'N'
           AND non.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id        = non.svcbr_id
                         AND rxh.prescription_id = non.prescription_id
                         AND rxh.refill_no       = non.refill_no
                     )
           AND NOT EXISTS(SELECT 1
                            FROM thot.compounding_detail rxh
                           WHERE rxh.svcbr_id        = non.svcbr_id
                             AND rxh.prescription_id = non.prescription_id
                             AND rxh.refill_no       = non.refill_no
                             AND rxh.line_id         = non.line_id
                             AND rxh.inventory_id    = non.inventory_id
                             AND rxh.lot             = non.lot
                           )
      ORDER BY ROWID;

    TYPE compounding_detail_tab IS TABLE OF compounding_detail_cur%ROWTYPE;

    TYPE compounding_detail_rowid_tab IS TABLE OF ROWID;

    t_compounding_detail        compounding_detail_tab;
    t_compounding_detail_rowid  compounding_detail_rowid_tab;
    t_triggers                  triggers_tab;
    v_context                   rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_COMPOUNDING_DETAIL';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'COMPOUNDING_DETAIL', t_triggers => t_triggers);

    OPEN compounding_detail_cur;

    OPEN compounding_detail_rowid_cur;

    LOOP
      FETCH compounding_detail_cur
      BULK COLLECT INTO t_compounding_detail LIMIT bulk_limit;

      EXIT WHEN t_compounding_detail.COUNT = 0;

      FETCH compounding_detail_rowid_cur
      BULK COLLECT INTO t_compounding_detail_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_compounding_detail.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT svcbr_id
                            , prescription_id
                            , refill_no
                            , compounded_date
                            , inventory_id
                            , quantity_used
                            , ob
                            , lot
                            , quantity_per
                            , no_days
                            , no_doses
                            , partnership_security_label
                            , creation_date
                            , created_by
                            , update_date
                            , updated_by
                            , line_id
                            , rxsub_id
                            , dm_flag
                            , tpn_drug_id
                            , tpn_pool_bag_flag
                            , number_of_items
                         FROM thot.compounding_detail)
               VALUES t_compounding_detail(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.COMPOUNDING_DETAIL'
                                                             ,p_run_id     => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_compounding_detail.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT svcbr_id
                                  , prescription_id
                                  , refill_no
                                  , compounded_date
                                  , inventory_id
                                  , quantity_used
                                  , ob
                                  , lot
                                  , quantity_per
                                  , no_days
                                  , no_doses
                                  , partnership_security_label
                                  , creation_date
                                  , created_by
                                  , update_date
                                  , updated_by
                                  , line_id
                                  , rxsub_id
                                  , dm_flag
                                  , tpn_drug_id
                                  , tpn_pool_bag_flag
                                  , number_of_items
                               FROM thot.compounding_detail)
                     VALUES t_compounding_detail(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'SVCBR_ID|PRESCRIPTION_ID|REFILL_NO'
                                                               ,p_attr1_value   =>    t_compounding_detail(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).svcbr_id
                                                                                   || '|'
                                                                                   || t_compounding_detail(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).prescription_id
                                                                                   || '|'
                                                                                   || t_compounding_detail(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).refill_no
                                                               ,p_attr2_name    => 'INVENTORY_ID'
                                                               ,p_attr2_value   => t_compounding_detail(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).inventory_id
                                                               ,p_attr3_name    => 'LOT'
                                                               ,p_attr3_value   => t_compounding_detail(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).lot
                                                               ,p_run_id     => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_compounding_detail_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context   => v_context,
                                              p_error_msg => 'Records processed :' || t_compounding_detail.COUNT || ' | Records inserted:' ||(t_compounding_detail.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                            , p_run_id => g_run_id);

      FOR i IN 1 .. t_compounding_detail.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.compounding_detail@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_compounding_detail_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE compounding_detail_cur;

    CLOSE compounding_detail_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF compounding_detail_cur%ISOPEN THEN
        CLOSE compounding_detail_cur;
      END IF;

      IF compounding_detail_rowid_cur%ISOPEN THEN
        CLOSE compounding_detail_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_compounding_detail;
--================================================================
--| PULL_ANCILLARY_PRESCRIPTIONS: Pulls ANCILLARY_PRESCRIPTIONS table to rxhome.             |
--================================================================
  PROCEDURE pull_ancillary_prescriptions
  IS
    CURSOR ancillary_prescriptions_cur
    IS
      SELECT   svcbr_id
              ,prescription_id
              ,refill_no
              ,inventory_id
              ,quantity_per
              ,no_days
              ,quantity_to_ship
              ,anc_labels_print
              ,ob
              ,lot
              ,rxsub_id
              ,no_doses
              ,partnership_security_label
              ,creation_date
              ,created_by
              ,update_date
              ,updated_by
          FROM rxh_stage.ancillary_prescriptions@rxh_stg arx
         WHERE arx.processed_flag = 'N'
           AND arx.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = arx.svcbr_id
                         AND rxh.prescription_id = arx.prescription_id
                         AND rxh.refill_no = arx.refill_no)
           AND NOT EXISTS(SELECT 1
                            FROM thot.ancillary_prescriptions rxh
                           WHERE rxh.svcbr_id = arx.svcbr_id
                             AND rxh.prescription_id = arx.prescription_id
                             AND rxh.refill_no = arx.refill_no
                             AND rxh.inventory_id = arx.inventory_id
                             AND rxh.lot = arx.lot)
      ORDER BY ROWID;

    CURSOR ancillary_rx_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.ancillary_prescriptions@rxh_stg arx
         WHERE arx.processed_flag = 'N'
           AND arx.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.ancillary_prescriptions rxh
                           WHERE rxh.svcbr_id = arx.svcbr_id
                             AND rxh.prescription_id = arx.prescription_id
                             AND rxh.refill_no = arx.refill_no
                             AND rxh.inventory_id = arx.inventory_id
                             AND rxh.lot = arx.lot)
      ORDER BY ROWID;

    TYPE ancillary_prescriptions_tab IS TABLE OF ancillary_prescriptions_cur%ROWTYPE;

    TYPE ancillary_rx_rowid_tab IS TABLE OF ROWID;

    t_ancillary_prescriptions  ancillary_prescriptions_tab;
    t_ancillary_rx_rowid       ancillary_rx_rowid_tab;
    t_triggers                 triggers_tab;
    v_context                  rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_ANCILLARY_PRESCRIPTIONS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'ANCILLARY_PRESCRIPTIONS', t_triggers => t_triggers);

    OPEN ancillary_prescriptions_cur;

    OPEN ancillary_rx_rowid_cur;

    LOOP
      FETCH ancillary_prescriptions_cur
      BULK COLLECT INTO t_ancillary_prescriptions LIMIT bulk_limit;

      EXIT WHEN t_ancillary_prescriptions.COUNT = 0;

      FETCH ancillary_rx_rowid_cur
      BULK COLLECT INTO t_ancillary_rx_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_ancillary_prescriptions.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT svcbr_id
                             ,prescription_id
                             ,refill_no
                             ,inventory_id
                             ,quantity_per
                             ,no_days
                             ,quantity_to_ship
                             ,anc_labels_print
                             ,ob
                             ,lot
                             ,rxsub_id
                             ,no_doses
                             ,partnership_security_label
                             ,creation_date
                             ,created_by
                             ,update_date
                             ,updated_by
                         FROM thot.ancillary_prescriptions)
               VALUES t_ancillary_prescriptions(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.ANCILLARY_PRESCRIPTIONS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_ancillary_prescriptions.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT svcbr_id
                                   ,prescription_id
                                   ,refill_no
                                   ,inventory_id
                                   ,quantity_per
                                   ,no_days
                                   ,quantity_to_ship
                                   ,anc_labels_print
                                   ,ob
                                   ,lot
                                   ,rxsub_id
                                   ,no_doses
                                   ,partnership_security_label
                                   ,creation_date
                                   ,created_by
                                   ,update_date
                                   ,updated_by
                               FROM thot.ancillary_prescriptions)
                     VALUES t_ancillary_prescriptions(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'SVCBR_ID|PRESCRIPTION_ID|REFILL_NO'
                                                               ,p_attr1_value   =>    t_ancillary_prescriptions(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).svcbr_id
                                                                                   || '|'
                                                                                   || t_ancillary_prescriptions(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).prescription_id
                                                                                   || '|'
                                                                                   || t_ancillary_prescriptions(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).refill_no
                                                               ,p_attr2_name    => 'INVENTORY_ID'
                                                               ,p_attr2_value   => t_ancillary_prescriptions(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).inventory_id
                                                               ,p_attr3_name    => 'LOT'
                                                               ,p_attr3_value   => t_ancillary_prescriptions(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).lot
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_ancillary_rx_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_ancillary_prescriptions.COUNT || ' | Records inserted:' ||(t_ancillary_prescriptions.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_ancillary_prescriptions.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.ancillary_prescriptions@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_ancillary_rx_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE ancillary_prescriptions_cur;

    CLOSE ancillary_rx_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF ancillary_prescriptions_cur%ISOPEN THEN
        CLOSE ancillary_prescriptions_cur;
      END IF;

      IF ancillary_rx_rowid_cur%ISOPEN THEN
        CLOSE ancillary_rx_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_ancillary_prescriptions;

--========================================
--| PULL_SHIPMENTS: Pulls SHIPMENTS table to rxhome.         |
--========================================
  PROCEDURE pull_shipments
  IS
    CURSOR shipments_cur
    IS
      SELECT   ID
              ,prior_id
              ,next_id
              ,from_cmp_id
              ,from_svcbr_id
              ,from_loc_type
              ,from_loc_id
              ,to_type
              ,to_id
              ,shipment_date
              ,shipping_method_abbrev
              ,tracking_no
              ,comments
              ,completed_date
              ,completed_xuser
              ,invoicer
              ,invoiced_flag
              ,completed_flag
              ,to_svcbr_id
              ,srs_completed_date
              ,srs_completed_xuser
              ,to_addr_seq
              ,batch_no
              ,dvr_sign_xuser
              ,dvr_sign_xdate
              ,shipping_id
              ,partnership_security_label
              ,batched_by
              ,batched_date
              ,pac_interface_flag
              ,srs_auth_type
              ,srs_auth_code
              ,ncpdp_dur_review_completed
              ,srs_shipment_datetime
              ,CREATED_BY     --[ 16.5 Tier 6  JCORONA  ADDING COLUMN
              ,CREATION_DATE  --[ 16.5 Tier 6  JCORONA  ADDING COLUMN
          FROM rxh_stage.shipments@rxh_stg shp
         WHERE shp.processed_flag = 'N'
           AND shp.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = shp.to_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.shipments rxh
                           WHERE rxh.ID = shp.ID)
      ORDER BY ROWID;

    CURSOR shipments_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.shipments@rxh_stg shp
         WHERE shp.processed_flag = 'N'
           AND shp.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = shp.to_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.shipments rxh
                           WHERE rxh.ID = shp.ID)
      ORDER BY ROWID;

    TYPE shipments_tab IS TABLE OF shipments_cur%ROWTYPE;

    TYPE shipments_rowid_tab IS TABLE OF ROWID;

    t_shipments        shipments_tab;
    t_shipments_rowid  shipments_rowid_tab;
    t_triggers         triggers_tab;
    v_context          rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_SHIPMENTS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'SHIPMENTS', t_triggers => t_triggers);

    OPEN shipments_cur;

    OPEN shipments_rowid_cur;

    LOOP
      FETCH shipments_cur
      BULK COLLECT INTO t_shipments LIMIT bulk_limit;

      EXIT WHEN t_shipments.COUNT = 0;

      FETCH shipments_rowid_cur
      BULK COLLECT INTO t_shipments_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_shipments.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT ID
                             ,prior_id
                             ,next_id
                             ,from_cmp_id
                             ,from_svcbr_id
                             ,from_loc_type
                             ,from_loc_id
                             ,to_type
                             ,to_id
                             ,shipment_date
                             ,shipping_method_abbrev
                             ,tracking_no
                             ,comments
                             ,completed_date
                             ,completed_xuser
                             ,invoicer
                             ,invoiced_flag
                             ,completed_flag
                             ,to_svcbr_id
                             ,srs_completed_date
                             ,srs_completed_xuser
                             ,to_addr_seq
                             ,batch_no
                             ,dvr_sign_xuser
                             ,dvr_sign_xdate
                             ,shipping_id
                             ,partnership_security_label
                             ,batched_by
                             ,batched_date
                             ,pac_interface_flag
                             ,srs_auth_type
                             ,srs_auth_code
                             ,ncpdp_dur_review_completed
                             ,srs_shipment_datetime
                             ,CREATED_BY     --[ 16.5 Tier 6  JCORONA  ADDING COLUMN
                             ,CREATION_DATE  --[ 16.5 Tier 6  JCORONA  ADDING COLUMN
                         FROM thot.shipments)
               VALUES t_shipments(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.SHIPMENTS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_shipments.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT ID
                                   ,prior_id
                                   ,next_id
                                   ,from_cmp_id
                                   ,from_svcbr_id
                                   ,from_loc_type
                                   ,from_loc_id
                                   ,to_type
                                   ,to_id
                                   ,shipment_date
                                   ,shipping_method_abbrev
                                   ,tracking_no
                                   ,comments
                                   ,completed_date
                                   ,completed_xuser
                                   ,invoicer
                                   ,invoiced_flag
                                   ,completed_flag
                                   ,to_svcbr_id
                                   ,srs_completed_date
                                   ,srs_completed_xuser
                                   ,to_addr_seq
                                   ,batch_no
                                   ,dvr_sign_xuser
                                   ,dvr_sign_xdate
                                   ,shipping_id
                                   ,partnership_security_label
                                   ,batched_by
                                   ,batched_date
                                   ,pac_interface_flag
                                   ,srs_auth_type
                                   ,srs_auth_code
                                   ,ncpdp_dur_review_completed
                                   ,srs_shipment_datetime
                                   ,CREATED_BY     --[ 16.5 Tier 6  JCORONA  ADDING COLUMN
                                   ,CREATION_DATE  --[ 16.5 Tier 6  JCORONA  ADDING COLUMN
                               FROM thot.shipments)
                     VALUES t_shipments(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'ID'
                                                               ,p_attr1_value   => t_shipments(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).ID
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_shipments_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_shipments.COUNT || ' | Records inserted:' ||(t_shipments.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_shipments.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.shipments@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_shipments_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE shipments_cur;

    CLOSE shipments_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF shipments_cur%ISOPEN THEN
        CLOSE shipments_cur;
      END IF;

      IF shipments_rowid_cur%ISOPEN THEN
        CLOSE shipments_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_shipments;

--================================================
--| PULL_SHIPMENT_ITEMS: Pulls SHIPMENT_ITEMS table to rxhome.         |
--================================================
  PROCEDURE pull_shipment_items
  IS
    CURSOR shipment_items_cur
    IS
      SELECT   shipment_id
              ,inventory_id
              ,lot
              ,quantity
              ,so_id
              ,rx_svcbr_id
              ,rx_prescription_id
              ,rx_refill_no
              ,nv_nurse_id
              ,nv_patient_id
              ,nv_start_time
              ,ob
              ,item_source
              ,comments
              ,COST
              ,porc
              ,payor_id
              ,list_price
              ,req_header_id_seq
              ,billing_group_seq
              ,partnership_security_label
              ,ncpdp_svc_prescriber_seq
              ,created_by
              ,creation_date
              ,updated_by
              ,updated_date
          FROM rxh_stage.shipment_items@rxh_stg shp
         WHERE shp.processed_flag = 'N'
           AND shp.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.shipments rxh
                       WHERE rxh.ID = shp.shipment_id)
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = shp.rx_svcbr_id
                         AND rxh.prescription_id = shp.rx_prescription_id
                         AND rxh.refill_no = shp.rx_refill_no)
      ORDER BY ROWID;

    CURSOR shipment_items_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.shipment_items@rxh_stg shp
         WHERE shp.processed_flag = 'N'
           AND shp.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.shipments rxh
                       WHERE rxh.ID = shp.shipment_id)
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = shp.rx_svcbr_id
                         AND rxh.prescription_id = shp.rx_prescription_id
                         AND rxh.refill_no = shp.rx_refill_no)
      ORDER BY ROWID;

    TYPE shipment_items_tab IS TABLE OF shipment_items_cur%ROWTYPE;

    TYPE shipment_items_rowid_tab IS TABLE OF ROWID;

    t_shipment_items        shipment_items_tab;
    t_shipment_items_rowid  shipment_items_rowid_tab;
    t_triggers              triggers_tab;
    v_context               rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_SHIPMENT_ITEMS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'SHIPMENT_ITEMS', t_triggers => t_triggers);

    OPEN shipment_items_cur;

    OPEN shipment_items_rowid_cur;

    LOOP
      FETCH shipment_items_cur
      BULK COLLECT INTO t_shipment_items LIMIT bulk_limit;

      EXIT WHEN t_shipment_items.COUNT = 0;

      FETCH shipment_items_rowid_cur
      BULK COLLECT INTO t_shipment_items_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_shipment_items.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT shipment_id
                             ,inventory_id
                             ,lot
                             ,quantity
                             ,so_id
                             ,rx_svcbr_id
                             ,rx_prescription_id
                             ,rx_refill_no
                             ,nv_nurse_id
                             ,nv_patient_id
                             ,nv_start_time
                             ,ob
                             ,item_source
                             ,comments
                             ,COST
                             ,porc
                             ,payor_id
                             ,list_price
                             ,req_header_id_seq
                             ,billing_group_seq
                             ,partnership_security_label
                             ,ncpdp_svc_prescriber_seq
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,updated_date
                         FROM thot.shipment_items)
               VALUES t_shipment_items(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.SHIPMENT_ITEMS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_shipment_items.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT shipment_id
                                   ,inventory_id
                                   ,lot
                                   ,quantity
                                   ,so_id
                                   ,rx_svcbr_id
                                   ,rx_prescription_id
                                   ,rx_refill_no
                                   ,nv_nurse_id
                                   ,nv_patient_id
                                   ,nv_start_time
                                   ,ob
                                   ,item_source
                                   ,comments
                                   ,COST
                                   ,porc
                                   ,payor_id
                                   ,list_price
                                   ,req_header_id_seq
                                   ,billing_group_seq
                                   ,partnership_security_label
                                   ,ncpdp_svc_prescriber_seq
                                   ,created_by
                                   ,creation_date
                                   ,updated_by
                                   ,updated_date
                               FROM thot.shipment_items)
                     VALUES t_shipment_items(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'SHIPMENT_ID'
                                                               ,p_attr1_value   => t_shipment_items(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).shipment_id
                                                               ,p_attr2_name    => 'INVENTORY_ID'
                                                               ,p_attr2_value   => t_shipment_items(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).inventory_id
                                                               ,p_attr3_name    => 'LOT'
                                                               ,p_attr3_value   => t_shipment_items(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).lot
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_shipment_items_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_shipment_items.COUNT || ' | Records inserted:' ||(t_shipment_items.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_shipment_items.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.shipment_items@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_shipment_items_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE shipment_items_cur;

    CLOSE shipment_items_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF shipment_items_cur%ISOPEN THEN
        CLOSE shipment_items_cur;
      END IF;

      IF shipment_items_rowid_cur%ISOPEN THEN
        CLOSE shipment_items_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_shipment_items;

--=============================================+
--| PULL_SHIPMENT_RXS: Pulls SHIPMENT_RXS table to rxhome.          |
--=============================================+
  PROCEDURE pull_shipment_rxs
  IS
    CURSOR shipment_rxs_cur
    IS
      SELECT   shipment_id
              ,svcbr_id
              ,prescription_id
              ,refill_no
              ,partnership_security_label
              ,shipping_id
          FROM rxh_stage.shipment_rxs@rxh_stg shp
         WHERE shp.processed_flag = 'N'
           AND shp.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.shipments rxh
                       WHERE rxh.ID = shp.shipment_id)
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = shp.svcbr_id
                         AND rxh.prescription_id = shp.prescription_id
                         AND rxh.refill_no = shp.refill_no)
      ORDER BY ROWID;

    CURSOR shipment_rxs_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.shipment_rxs@rxh_stg shp
         WHERE shp.processed_flag = 'N'
           AND shp.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.shipments rxh
                       WHERE rxh.ID = shp.shipment_id)
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = shp.svcbr_id
                         AND rxh.prescription_id = shp.prescription_id
                         AND rxh.refill_no = shp.refill_no)
      ORDER BY ROWID;

    TYPE shipment_rxs_tab IS TABLE OF shipment_rxs_cur%ROWTYPE;

    TYPE shipment_rxs_rowid_tab IS TABLE OF ROWID;

    t_shipment_rxs        shipment_rxs_tab;
    t_shipment_rxs_rowid  shipment_rxs_rowid_tab;
    t_triggers            triggers_tab;
    v_context             rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_SHIPMENT_RXS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'SHIPMENT_RXS', t_triggers => t_triggers);

    OPEN shipment_rxs_cur;

    OPEN shipment_rxs_rowid_cur;

    LOOP
      FETCH shipment_rxs_cur
      BULK COLLECT INTO t_shipment_rxs LIMIT bulk_limit;

      EXIT WHEN t_shipment_rxs.COUNT = 0;

      FETCH shipment_rxs_rowid_cur
      BULK COLLECT INTO t_shipment_rxs_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_shipment_rxs.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT shipment_id
                             ,svcbr_id
                             ,prescription_id
                             ,refill_no
                             ,partnership_security_label
                             ,shipping_id
                         FROM thot.shipment_rxs)
               VALUES t_shipment_rxs(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.SHIPMENT_RXS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_shipment_rxs.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT shipment_id
                                   ,svcbr_id
                                   ,prescription_id
                                   ,refill_no
                                   ,partnership_security_label
                                   ,shipping_id
                               FROM thot.shipment_rxs)
                     VALUES t_shipment_rxs(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'SVCBR_ID|PRESCRIPTION_ID|REFILL_NO'
                                                               ,p_attr1_value   =>    t_shipment_rxs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).svcbr_id
                                                                                   || '|'
                                                                                   || t_shipment_rxs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).prescription_id
                                                                                   || '|'
                                                                                   || t_shipment_rxs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).refill_no
                                                               ,p_attr2_name    => 'SHIPMENT_ID'
                                                               ,p_attr2_value   => t_shipment_rxs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).shipment_id
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_shipment_rxs_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_shipment_rxs.COUNT || ' | Records inserted:' ||(t_shipment_rxs.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_shipment_rxs.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.shipment_rxs@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_shipment_rxs_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE shipment_rxs_cur;

    CLOSE shipment_rxs_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF shipment_rxs_cur%ISOPEN THEN
        CLOSE shipment_rxs_cur;
      END IF;

      IF shipment_rxs_rowid_cur%ISOPEN THEN
        CLOSE shipment_rxs_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_shipment_rxs;

--======================================
--| PULL_SHIPPING: Pulls SHIPPING table to rxhome.           |
--======================================
  PROCEDURE pull_shipping
  IS
    CURSOR shipping_cur
    IS
      SELECT   ID
              ,cmp_id
              ,svcbr_id
              ,to_type
              ,to_id
              ,to_addr_seq
              ,shipper
              ,shipper_method
              ,ship_date
              ,calc_weight
              ,signature_required
              ,saturday_delivery
              ,residential
              ,PACKAGES
              ,weight
              ,status
              ,manual_flag
              ,shipment_id
              ,ERROR_CODE
              ,error_msg
              ,meter_no
              ,net_charge
              ,instructions
              ,ACCOUNT
              ,REFERENCE
              ,dry_ice_weight
              ,name_flag
              ,created_by
              ,created_date
              ,shipping_status
              ,rule_no
              ,updated_by_user
              ,updated_by_date
              ,actual_shipper_method
              ,actual_shipper
              ,pac_ready_flag
              ,pac_interface_flag
              ,pac_declared_value
              ,srvc_ver_msg
              ,srvc_ver_status
              ,srvc_ver_user
              ,srvc_ver_date
              ,pac_priority_flag
              ,pac_emergency_ship_flag
              ,pac_fep_ship_flag
              ,print_delivery_inst
          FROM rxh_stage.shipping@rxh_stg shp
         WHERE shp.processed_flag = 'N'
           AND shp.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = shp.to_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.shipping rxh
                           WHERE rxh.ID = shp.ID)
      ORDER BY ROWID;

    CURSOR shipping_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.shipping@rxh_stg shp
         WHERE shp.processed_flag = 'N'
           AND shp.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = shp.to_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.shipping rxh
                           WHERE rxh.ID = shp.ID)
      ORDER BY ROWID;

    TYPE shipping_tab IS TABLE OF shipping_cur%ROWTYPE;

    TYPE shipping_rowid_tab IS TABLE OF ROWID;

    t_shipping        shipping_tab;
    t_shipping_rowid  shipping_rowid_tab;
    t_triggers        triggers_tab;
    v_context         rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_SHIPPING';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'SHIPPING', t_triggers => t_triggers);

    OPEN shipping_cur;

    OPEN shipping_rowid_cur;

    LOOP
      FETCH shipping_cur
      BULK COLLECT INTO t_shipping LIMIT bulk_limit;

      EXIT WHEN t_shipping.COUNT = 0;

      FETCH shipping_rowid_cur
      BULK COLLECT INTO t_shipping_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_shipping.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT ID
                             ,cmp_id
                             ,svcbr_id
                             ,to_type
                             ,to_id
                             ,to_addr_seq
                             ,shipper
                             ,shipper_method
                             ,ship_date
                             ,calc_weight
                             ,signature_required
                             ,saturday_delivery
                             ,residential
                             ,PACKAGES
                             ,weight
                             ,status
                             ,manual_flag
                             ,shipment_id
                             ,ERROR_CODE
                             ,error_msg
                             ,meter_no
                             ,net_charge
                             ,instructions
                             ,ACCOUNT
                             ,REFERENCE
                             ,dry_ice_weight
                             ,name_flag
                             ,created_by
                             ,created_date
                             ,shipping_status
                             ,rule_no
                             ,updated_by_user
                             ,updated_by_date
                             ,actual_shipper_method
                             ,actual_shipper
                             ,pac_ready_flag
                             ,pac_interface_flag
                             ,pac_declared_value
                             ,srvc_ver_msg
                             ,srvc_ver_status
                             ,srvc_ver_user
                             ,srvc_ver_date
                             ,pac_priority_flag
                             ,pac_emergency_ship_flag
                             ,pac_fep_ship_flag
                             ,print_delivery_inst
                         FROM thot.shipping)
               VALUES t_shipping(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.SHIPPING'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_shipping.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT ID
                                   ,cmp_id
                                   ,svcbr_id
                                   ,to_type
                                   ,to_id
                                   ,to_addr_seq
                                   ,shipper
                                   ,shipper_method
                                   ,ship_date
                                   ,calc_weight
                                   ,signature_required
                                   ,saturday_delivery
                                   ,residential
                                   ,PACKAGES
                                   ,weight
                                   ,status
                                   ,manual_flag
                                   ,shipment_id
                                   ,ERROR_CODE
                                   ,error_msg
                                   ,meter_no
                                   ,net_charge
                                   ,instructions
                                   ,ACCOUNT
                                   ,REFERENCE
                                   ,dry_ice_weight
                                   ,name_flag
                                   ,created_by
                                   ,created_date
                                   ,shipping_status
                                   ,rule_no
                                   ,updated_by_user
                                   ,updated_by_date
                                   ,actual_shipper_method
                                   ,actual_shipper
                                   ,pac_ready_flag
                                   ,pac_interface_flag
                                   ,pac_declared_value
                                   ,srvc_ver_msg
                                   ,srvc_ver_status
                                   ,srvc_ver_user
                                   ,srvc_ver_date
                                   ,pac_priority_flag
                                   ,pac_emergency_ship_flag
                                   ,pac_fep_ship_flag
                                   ,print_delivery_inst
                               FROM thot.shipping)
                     VALUES t_shipping(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'ID'
                                                               ,p_attr1_value   => t_shipping(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).ID
                                                               ,p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_shipping_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_shipping.COUNT || ' | Records inserted:' ||(t_shipping.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_shipping.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.shipping@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_shipping_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE shipping_cur;

    CLOSE shipping_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF shipping_cur%ISOPEN THEN
        CLOSE shipping_cur;
      END IF;

      IF shipping_rowid_cur%ISOPEN THEN
        CLOSE shipping_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_shipping;

--=============================================
--| PULL_SHIPPING_DTL: Pulls SHIPPING_DTL table to rxhome.           |
--=============================================
  PROCEDURE pull_shipping_dtl
  IS
    CURSOR shipping_dtl_cur
    IS
      SELECT   shipping_id
              ,package_no
              ,packaging
              ,box_type
              ,height
              ,width
              ,LENGTH
              ,weight
              ,declared_value
              ,tracking_no
              ,delivery_date
              ,net_charge
              ,billed_weight
              ,signed_for
              ,address_barcode
              ,dest_ramp_id
              ,ursa
              ,astra
              ,service_commitment_code
              ,dry_ice_weight
              ,created_by
              ,created_date
              ,updated_by
              ,updated_date
              ,ERROR_CODE
              ,error_msg
              ,status
              ,pac_pkg_id
          FROM rxh_stage.shipping_dtl@rxh_stg shp
         WHERE shp.processed_flag = 'N'
           AND shp.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.shipping rxh
                       WHERE rxh.ID = shp.shipping_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.shipping_dtl rxh
                           WHERE rxh.shipping_id = shp.shipping_id
                             AND rxh.package_no = shp.package_no)
      ORDER BY ROWID;

    CURSOR shipping_dtl_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.shipping_dtl@rxh_stg shp
         WHERE shp.processed_flag = 'N'
           AND shp.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.shipping rxh
                       WHERE rxh.ID = shp.shipping_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.shipping_dtl rxh
                           WHERE rxh.shipping_id = shp.shipping_id
                             AND rxh.package_no = shp.package_no)
      ORDER BY ROWID;

    TYPE shipping_dtl_tab IS TABLE OF shipping_dtl_cur%ROWTYPE;

    TYPE shipping_dtl_rowid_tab IS TABLE OF ROWID;

    t_shipping_dtl        shipping_dtl_tab;
    t_shipping_dtl_rowid  shipping_dtl_rowid_tab;
    t_triggers            triggers_tab;
    v_context             rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_SHIPPING_DTL';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'SHIPPING_DTL', t_triggers => t_triggers);

    OPEN shipping_dtl_cur;

    OPEN shipping_dtl_rowid_cur;

    LOOP
      FETCH shipping_dtl_cur
      BULK COLLECT INTO t_shipping_dtl LIMIT bulk_limit;

      EXIT WHEN t_shipping_dtl.COUNT = 0;

      FETCH shipping_dtl_rowid_cur
      BULK COLLECT INTO t_shipping_dtl_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_shipping_dtl.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT shipping_id
                             ,package_no
                             ,packaging
                             ,box_type
                             ,height
                             ,width
                             ,LENGTH
                             ,weight
                             ,declared_value
                             ,tracking_no
                             ,delivery_date
                             ,net_charge
                             ,billed_weight
                             ,signed_for
                             ,address_barcode
                             ,dest_ramp_id
                             ,ursa
                             ,astra
                             ,service_commitment_code
                             ,dry_ice_weight
                             ,created_by
                             ,created_date
                             ,updated_by
                             ,updated_date
                             ,ERROR_CODE
                             ,error_msg
                             ,status
                             ,pac_pkg_id
                         FROM thot.shipping_dtl)
               VALUES t_shipping_dtl(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.SHIPPING_DTL'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_shipping_dtl.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT shipping_id
                                   ,package_no
                                   ,packaging
                                   ,box_type
                                   ,height
                                   ,width
                                   ,LENGTH
                                   ,weight
                                   ,declared_value
                                   ,tracking_no
                                   ,delivery_date
                                   ,net_charge
                                   ,billed_weight
                                   ,signed_for
                                   ,address_barcode
                                   ,dest_ramp_id
                                   ,ursa
                                   ,astra
                                   ,service_commitment_code
                                   ,dry_ice_weight
                                   ,created_by
                                   ,created_date
                                   ,updated_by
                                   ,updated_date
                                   ,ERROR_CODE
                                   ,error_msg
                                   ,status
                                   ,pac_pkg_id
                               FROM thot.shipping_dtl)
                     VALUES t_shipping_dtl(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'SHIPPING_ID'
                                                               ,p_attr1_value   => t_shipping_dtl(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).shipping_id
                                                               ,p_attr2_name    => 'PACKAGE_NO'
                                                               ,p_attr2_value   => t_shipping_dtl(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).package_no
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_shipping_dtl_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_shipping_dtl.COUNT || ' | Records inserted:' ||(t_shipping_dtl.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_shipping_dtl.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.shipping_dtl@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_shipping_dtl_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE shipping_dtl_cur;

    CLOSE shipping_dtl_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF shipping_dtl_cur%ISOPEN THEN
        CLOSE shipping_dtl_cur;
      END IF;

      IF shipping_dtl_rowid_cur%ISOPEN THEN
        CLOSE shipping_dtl_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_shipping_dtl;

--==================================================
--| PULL_SHIPPING_DTL_RXS: Pulls SHIPPING_DTL_RX table to rxhome.         |
--==================================================
  PROCEDURE pull_shipping_dtl_rxs
  IS
    CURSOR shipping_dtl_rxs_cur
    IS
      SELECT   shipping_id
              ,package_no
              ,svcbr_id
              ,prescription_id
              ,refill_no
              ,created_by
              ,created_date
              ,updated_by
              ,updated_date
          FROM rxh_stage.shipping_dtl_rxs@rxh_stg shp
         WHERE shp.processed_flag = 'N'
           AND shp.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.shipping rxh
                       WHERE rxh.ID = shp.shipping_id)
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = shp.svcbr_id
                         AND rxh.prescription_id = shp.prescription_id
                         AND rxh.refill_no = shp.refill_no)
           AND NOT EXISTS(SELECT 1
                            FROM thot.shipping_dtl_rxs rxh
                           WHERE rxh.shipping_id = shp.shipping_id
                             AND rxh.package_no = shp.package_no
                             AND rxh.svcbr_id = shp.svcbr_id
                             AND rxh.prescription_id = shp.prescription_id
                             AND rxh.refill_no = shp.refill_no)
      ORDER BY ROWID;

    CURSOR shipping_dtl_rxs_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.shipping_dtl_rxs@rxh_stg shp
         WHERE shp.processed_flag = 'N'
           AND shp.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.shipping rxh
                       WHERE rxh.ID = shp.shipping_id)
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = shp.svcbr_id
                         AND rxh.prescription_id = shp.prescription_id
                         AND rxh.refill_no = shp.refill_no)
           AND NOT EXISTS(SELECT 1
                            FROM thot.shipping_dtl_rxs rxh
                           WHERE rxh.shipping_id = shp.shipping_id
                             AND rxh.package_no = shp.package_no
                             AND rxh.svcbr_id = shp.svcbr_id
                             AND rxh.prescription_id = shp.prescription_id
                             AND rxh.refill_no = shp.refill_no)
      ORDER BY ROWID;

    TYPE shipping_dtl_rxs_tab IS TABLE OF shipping_dtl_rxs_cur%ROWTYPE;

    TYPE shipping_dtl_rxs_rowid_tab IS TABLE OF ROWID;

    t_shipping_dtl_rxs        shipping_dtl_rxs_tab;
    t_shipping_dtl_rxs_rowid  shipping_dtl_rxs_rowid_tab;
    t_triggers                triggers_tab;
    v_context                 rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_SHIPPING_DTL_RXS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'SHIPPING_DTL_RXS', t_triggers => t_triggers);

    OPEN shipping_dtl_rxs_cur;

    OPEN shipping_dtl_rxs_rowid_cur;

    LOOP
      FETCH shipping_dtl_rxs_cur
      BULK COLLECT INTO t_shipping_dtl_rxs LIMIT bulk_limit;

      EXIT WHEN t_shipping_dtl_rxs.COUNT = 0;

      FETCH shipping_dtl_rxs_rowid_cur
      BULK COLLECT INTO t_shipping_dtl_rxs_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_shipping_dtl_rxs.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT shipping_id
                             ,package_no
                             ,svcbr_id
                             ,prescription_id
                             ,refill_no
                             ,created_by
                             ,created_date
                             ,updated_by
                             ,updated_date
                         FROM thot.shipping_dtl_rxs)
               VALUES t_shipping_dtl_rxs(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.SHIPPING_DTL_RXS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_shipping_dtl_rxs.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT shipping_id
                                   ,package_no
                                   ,svcbr_id
                                   ,prescription_id
                                   ,refill_no
                                   ,created_by
                                   ,created_date
                                   ,updated_by
                                   ,updated_date
                               FROM thot.shipping_dtl_rxs)
                     VALUES t_shipping_dtl_rxs(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'SVCBR_ID|PRESCRIPTION_ID|REFILL_NO'
                                                               ,p_attr1_value   =>    t_shipping_dtl_rxs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).svcbr_id
                                                                                   || '|'
                                                                                   || t_shipping_dtl_rxs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).prescription_id
                                                                                   || '|'
                                                                                   || t_shipping_dtl_rxs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).refill_no
                                                               ,p_attr2_name    => 'SHIPPING_ID'
                                                               ,p_attr2_value   => t_shipping_dtl_rxs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).shipping_id
                                                               ,p_attr3_name    => 'PACKAGE_NO'
                                                               ,p_attr3_value   => t_shipping_dtl_rxs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).package_no
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_shipping_dtl_rxs_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_shipping_dtl_rxs.COUNT || ' | Records inserted:' ||(t_shipping_dtl_rxs.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_shipping_dtl_rxs.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.shipping_dtl_rxs@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_shipping_dtl_rxs_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE shipping_dtl_rxs_cur;

    CLOSE shipping_dtl_rxs_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF shipping_dtl_rxs_cur%ISOPEN THEN
        CLOSE shipping_dtl_rxs_cur;
      END IF;

      IF shipping_dtl_rxs_rowid_cur%ISOPEN THEN
        CLOSE shipping_dtl_rxs_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_shipping_dtl_rxs;

--===========================================
--| PULL_AUDIT_EVENTS: Pulls audit_events table to rxhome.        |
--===========================================
  PROCEDURE pull_audit_events
  IS
    CURSOR audit_events_cur
    IS
      SELECT   audit_type
              ,xuser
              ,xdate
              ,description
              ,table_name
              ,pkey
              ,SID
              ,serial_no
          FROM rxh_stage.audit_events@rxh_stg rxa
         WHERE rxa.processed_flag = 'N'
           AND rxa.run_id = g_run_id
      ORDER BY ROWID;

    CURSOR audit_events_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.audit_events@rxh_stg rxa
         WHERE rxa.processed_flag = 'N'
           AND rxa.run_id = g_run_id
      ORDER BY ROWID;

    TYPE audit_events_tab IS TABLE OF audit_events_cur%ROWTYPE;

    TYPE audit_events_rowid_tab IS TABLE OF ROWID;

    t_audit_events        audit_events_tab;
    t_audit_events_rowid  audit_events_rowid_tab;
    t_triggers            triggers_tab;
    v_context             rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_AUDIT_EVENTS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'AUDIT_EVENTS', t_triggers => t_triggers);

    OPEN audit_events_cur;

    OPEN audit_events_rowid_cur;

    LOOP
      FETCH audit_events_cur
      BULK COLLECT INTO t_audit_events LIMIT bulk_limit;

      EXIT WHEN t_audit_events.COUNT = 0;

      FETCH audit_events_rowid_cur
      BULK COLLECT INTO t_audit_events_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_audit_events.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT audit_type
                             ,xuser
                             ,xdate
                             ,description
                             ,table_name
                             ,pkey
                             ,SID
                             ,serial_no
                         FROM thot.audit_events)
               VALUES t_audit_events(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.AUDIT_EVENTS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_audit_events.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT audit_type
                                   ,xuser
                                   ,xdate
                                   ,description
                                   ,table_name
                                   ,pkey
                                   ,SID
                                   ,serial_no
                               FROM thot.audit_events)
                     VALUES t_audit_events(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_audit_events_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_audit_events.COUNT || ' | Records inserted:' ||(t_audit_events.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_audit_events.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.audit_events@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_audit_events_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE audit_events_cur;

    CLOSE audit_events_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF audit_events_cur%ISOPEN THEN
        CLOSE audit_events_cur;
      END IF;

      IF audit_events_rowid_cur%ISOPEN THEN
        CLOSE audit_events_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_audit_events;

--====================================+
--| PULL_RX_AUDIT: Pulls rx_audit table to rxhome.        |
--====================================+
  PROCEDURE pull_rx_audit
  IS
    CURSOR rx_audit_cur
    IS
      SELECT   svcbr_id
              ,prescription_id
              ,refill_no
              ,field_name
              ,char_value
              ,xuser
              ,xdate
              ,partnership_security_label
              ,accepted_flag
              ,tab_name
              ,old_char_value
              ,clinical_data_flag
              ,old_db_value
              ,new_db_value
              ,reference_id1
              ,reference_id2
          FROM rxh_stage.rx_audit@rxh_stg atg
         WHERE processed_flag = 'N'
           AND atg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = atg.svcbr_id
                         AND rxh.prescription_id = atg.prescription_id
                         AND rxh.refill_no = atg.refill_no)
      ORDER BY ROWID;

    CURSOR rx_audit_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.rx_audit@rxh_stg atg
         WHERE processed_flag = 'N'
           AND atg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = atg.svcbr_id
                         AND rxh.prescription_id = atg.prescription_id
                         AND rxh.refill_no = atg.refill_no)
      ORDER BY ROWID;

    TYPE rx_audit_tab IS TABLE OF rx_audit_cur%ROWTYPE;

    TYPE rx_audit_rowid_tab IS TABLE OF ROWID;

    t_rx_audit        rx_audit_tab;
    t_rx_audit_rowid  rx_audit_rowid_tab;
    t_triggers        triggers_tab;
    v_context         rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_RX_AUDIT';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'RX_AUDIT', t_triggers => t_triggers);

    OPEN rx_audit_cur;

    OPEN rx_audit_rowid_cur;

    LOOP
      FETCH rx_audit_cur
      BULK COLLECT INTO t_rx_audit LIMIT bulk_limit;

      EXIT WHEN t_rx_audit.COUNT = 0;

      FETCH rx_audit_rowid_cur
      BULK COLLECT INTO t_rx_audit_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_rx_audit.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT svcbr_id
                             ,prescription_id
                             ,refill_no
                             ,field_name
                             ,char_value
                             ,xuser
                             ,xdate
                             ,partnership_security_label
                             ,accepted_flag
                             ,tab_name
                             ,old_char_value
                             ,clinical_data_flag
                             ,old_db_value
                             ,new_db_value
                             ,reference_id1
                             ,reference_id2
                         FROM thot.rx_audit)
               VALUES t_rx_audit(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.RX_AUDIT'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_rx_audit.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT svcbr_id
                                   ,prescription_id
                                   ,refill_no
                                   ,field_name
                                   ,char_value
                                   ,xuser
                                   ,xdate
                                   ,partnership_security_label
                                   ,accepted_flag
                                   ,tab_name
                                   ,old_char_value
                                   ,clinical_data_flag
                                   ,old_db_value
                                   ,new_db_value
                                   ,reference_id1
                                   ,reference_id2
                               FROM thot.rx_audit)
                     VALUES t_rx_audit(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'SVCBR_ID|PRESCRIPTION_ID|REFILL_NO'
                                                               ,p_attr1_value   =>    t_rx_audit(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).svcbr_id
                                                                                   || '|'
                                                                                   || t_rx_audit(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).prescription_id
                                                                                   || '|'
                                                                                   || t_rx_audit(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).refill_no
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_rx_audit_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_rx_audit.COUNT || ' | Records inserted:' ||(t_rx_audit.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_rx_audit.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.rx_audit@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_rx_audit_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE rx_audit_cur;

    CLOSE rx_audit_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF rx_audit_cur%ISOPEN THEN
        CLOSE rx_audit_cur;
      END IF;

      IF rx_audit_rowid_cur%ISOPEN THEN
        CLOSE rx_audit_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_rx_audit;

--==============================================+
--| PULL_RX_REFILL_RULES: Pulls rx_refill_rules table to rxhome.           |
--==============================================+
  PROCEDURE pull_rx_refill_rules
  IS
    CURSOR rx_refill_rules_cur
    IS
      SELECT   svcbr_id
              ,prescription_id
              ,refill_rule
              ,VALUE
              ,reason
              ,drug_abbrev
              ,inventory_id
              ,xuser
              ,xdate
              ,partnership_security_label
          FROM rxh_stage.rx_refill_rules@rxh_stg atg
         WHERE atg.processed_flag = 'N'
           AND atg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = atg.svcbr_id
                         AND rxh.prescription_id = atg.prescription_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.rx_refill_rules rxh
                           WHERE rxh.svcbr_id = atg.svcbr_id
                             AND rxh.prescription_id = atg.prescription_id
                             AND rxh.xdate = atg.xdate)
      ORDER BY ROWID;

    CURSOR rx_refill_rules_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.rx_refill_rules@rxh_stg atg
         WHERE atg.processed_flag = 'N'
           AND atg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.prescriptions_table rxh
                       WHERE rxh.svcbr_id = atg.svcbr_id
                         AND rxh.prescription_id = atg.prescription_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.rx_refill_rules rxh
                           WHERE rxh.svcbr_id = atg.svcbr_id
                             AND rxh.prescription_id = atg.prescription_id
                             AND rxh.xdate = atg.xdate)
      ORDER BY ROWID;

    TYPE rx_refill_rules_tab IS TABLE OF rx_refill_rules_cur%ROWTYPE;

    TYPE rx_refill_rules_rowid_tab IS TABLE OF ROWID;

    t_rx_refill_rules        rx_refill_rules_tab;
    t_rx_refill_rules_rowid  rx_refill_rules_rowid_tab;
    t_triggers               triggers_tab;
    v_context                rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_RX_REFILL_RULES';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'RX_REFILL_RULES', t_triggers => t_triggers);

    OPEN rx_refill_rules_cur;

    OPEN rx_refill_rules_rowid_cur;

    LOOP
      FETCH rx_refill_rules_cur
      BULK COLLECT INTO t_rx_refill_rules LIMIT bulk_limit;

      EXIT WHEN t_rx_refill_rules.COUNT = 0;

      FETCH rx_refill_rules_rowid_cur
      BULK COLLECT INTO t_rx_refill_rules_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_rx_refill_rules.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT svcbr_id
                             ,prescription_id
                             ,refill_rule
                             ,VALUE
                             ,reason
                             ,drug_abbrev
                             ,inventory_id
                             ,xuser
                             ,xdate
                             ,partnership_security_label
                         FROM thot.rx_refill_rules)
               VALUES t_rx_refill_rules(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.RX_REFILL_RULES'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_rx_refill_rules.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT svcbr_id
                                   ,prescription_id
                                   ,refill_rule
                                   ,VALUE
                                   ,reason
                                   ,drug_abbrev
                                   ,inventory_id
                                   ,xuser
                                   ,xdate
                                   ,partnership_security_label
                               FROM thot.rx_refill_rules)
                     VALUES t_rx_refill_rules(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'SVCBR_ID|PRESCRIPTION_ID|XDATE'
                                                               ,p_attr1_value   =>    t_rx_refill_rules(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).svcbr_id
                                                                                   || '|'
                                                                                   || t_rx_refill_rules(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).prescription_id
                                                                                   || '|'
                                                                                   || t_rx_refill_rules(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).xdate
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_rx_refill_rules_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_rx_refill_rules.COUNT || ' | Records inserted:' ||(t_rx_refill_rules.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_rx_refill_rules.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.rx_refill_rules@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_rx_refill_rules_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE rx_refill_rules_cur;

    CLOSE rx_refill_rules_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF rx_refill_rules_cur%ISOPEN THEN
        CLOSE rx_refill_rules_cur;
      END IF;

      IF rx_refill_rules_rowid_cur%ISOPEN THEN
        CLOSE rx_refill_rules_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_rx_refill_rules;

--===================================================+
--| PULL_PATIENT_INSURANCE: Pulls patient_insurance table to rxhome.            |
--===================================================+
  PROCEDURE pull_patient_insurance
  IS
    CURSOR patient_insurance_cur
    IS
      SELECT   patient_id
              ,claim_center_no
              ,claim_center_seq
              ,effective_date
              ,expiration_date
              ,policy_no
              ,group_no
              ,authorization_no
              ,other
              ,subscriber_first
              ,subscriber_mi
              ,subscriber_last
              ,subscriber_generation
              ,subscriber_addr1
              ,subscriber_addr2
              ,subscriber_city
              ,subscriber_state
              ,subscriber_zip1
              ,subscriber_zip2
              ,subscriber_phone1
              ,subscriber_phone2
              ,subscriber_phone3
              ,subscriber_relation
              ,NULL AS subscriber_socsec
              ,subscriber_date_of_birth
              ,subscriber_sex
              ,subscriber_employee_id
              ,subscriber_employer
              ,subscriber_employer_addr1
              ,subscriber_employer_addr2
              ,subscriber_employer_city
              ,subscriber_employer_state
              ,subscriber_employer_zip1
              ,subscriber_employer_zip2
              ,employer_phone1
              ,employer_phone2
              ,employer_phone3
              ,plan_type
              ,plan_name
              ,assignment_flag
              ,deductible_amt
              ,percent_paid
              ,max_out
              ,lifetime_limit
              ,medicare_suppl
              ,bill30_start_date
              ,discount_percent
              ,terms
              ,billing_instruct1
              ,billing_instruct2
              ,billing_instruct3
              ,billing_instruct4
              ,billing_instruct5
              ,verification_date
              ,pricing_id
              ,dunning_letter_flag
              ,denial_form_required
              ,payor_type
              ,payor_id
              ,payor_addr_seq
              ,include_flag
              ,kit_type
              ,kit_code
              ,subscriber_country
              ,subscriber_employer_country
              ,edi_sub_id
              ,ccgroup
              ,pos_auto_send_flag
              ,billing_seq
              ,annual_limit
              ,benefit_terms
              ,pricing_guide
              ,copay_type_flag
              ,discount_type_flag
              ,billing_type
              ,copay
              ,person_code
              ,prior_auth_type
              ,ncpdp_pt_address_flag
              ,ncpdp_pregnancy_indicator_flag
              ,ncpdp_cardholder_flag
              ,ncpdp_group_id_flag
              ,ncpdp_other_coverage_code
              ,ncpdp_prescriber_name_flag
              ,ncpdp_compound_code
              ,ncpdp_unit_of_measure_code
              ,ncpdp_patient_location
              ,partnership_security_label
              ,ncpdp_copay_flag
              ,ncpdp_copay_porc
              ,ncpdp_copay_payor_id
              ,ncpdp_unit_dose_ind
              ,ncpdp_cmpd_dsg_form_desc
              ,ncpdp_cmpd_dspns_unit_form_ind
              ,ncpdp_cmpd_route_admin
              ,level_of_service
              ,employer_id
              ,subscriber_group_na
              ,socsec_crypted
              ,auto_send_270_flag
              ,nvisit_type
              ,do_not_autodial
              ,patient_residence
              ,pharm_service_type
              ,ncpdp_prescriber_fname_flag
              ,ncpdp_prescriber_address_flag
              ,ncpdp_prescriber_phone_flag
              ,ncpdp_prescriber_data_flag
              ,ncpdp_cob_other_coverage_code    --[ CQ22863 Adding cob fields
              ,ncpdp_cob_othr_payerid
              ,ncpdp_cob_o_payer_ptr_amt_code
              ,ncpdp_cob_othr_payer_covg_type
              ,ncpdp_cob_cardholder_flag        --] CQ22863
              ,ncpdp_cob_group_id_flag          --CQ22967
              ,created_by
              ,creation_date
              ,updated_by
              ,update_date
          FROM rxh_stage.patient_insurance@rxh_stg atg
         WHERE processed_flag = 'N'
           AND atg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = atg.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_insurance rxh
                           WHERE rxh.patient_id = atg.patient_id
                             AND rxh.claim_center_seq = atg.claim_center_seq)
      ORDER BY ROWID;

    CURSOR patient_insurance_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.patient_insurance@rxh_stg atg
         WHERE processed_flag = 'N'
           AND atg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = atg.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.patient_insurance rxh
                           WHERE rxh.patient_id = atg.patient_id
                             AND rxh.claim_center_seq = atg.claim_center_seq)
      ORDER BY ROWID;

    TYPE patient_insurance_tab IS TABLE OF patient_insurance_cur%ROWTYPE;

    TYPE patient_insurance_rowid_tab IS TABLE OF ROWID;

    t_patient_insurance        patient_insurance_tab;
    t_patient_insurance_rowid  patient_insurance_rowid_tab;
    t_triggers                 triggers_tab;
    v_context                  rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PATIENT_INSURANCE';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PATIENT_INSURANCE', t_triggers => t_triggers);

    OPEN patient_insurance_cur;

    OPEN patient_insurance_rowid_cur;

    LOOP
      FETCH patient_insurance_cur
      BULK COLLECT INTO t_patient_insurance LIMIT bulk_limit;

      EXIT WHEN t_patient_insurance.COUNT = 0;

      FETCH patient_insurance_rowid_cur
      BULK COLLECT INTO t_patient_insurance_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_patient_insurance.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT patient_id
                             ,claim_center_no
                             ,claim_center_seq
                             ,effective_date
                             ,expiration_date
                             ,policy_no
                             ,group_no
                             ,authorization_no
                             ,other
                             ,subscriber_first
                             ,subscriber_mi
                             ,subscriber_last
                             ,subscriber_generation
                             ,subscriber_addr1
                             ,subscriber_addr2
                             ,subscriber_city
                             ,subscriber_state
                             ,subscriber_zip1
                             ,subscriber_zip2
                             ,subscriber_phone1
                             ,subscriber_phone2
                             ,subscriber_phone3
                             ,subscriber_relation
                             ,subscriber_socsec
                             ,subscriber_date_of_birth
                             ,subscriber_sex
                             ,subscriber_employee_id
                             ,subscriber_employer
                             ,subscriber_employer_addr1
                             ,subscriber_employer_addr2
                             ,subscriber_employer_city
                             ,subscriber_employer_state
                             ,subscriber_employer_zip1
                             ,subscriber_employer_zip2
                             ,employer_phone1
                             ,employer_phone2
                             ,employer_phone3
                             ,plan_type
                             ,plan_name
                             ,assignment_flag
                             ,deductible_amt
                             ,percent_paid
                             ,max_out
                             ,lifetime_limit
                             ,medicare_suppl
                             ,bill30_start_date
                             ,discount_percent
                             ,terms
                             ,billing_instruct1
                             ,billing_instruct2
                             ,billing_instruct3
                             ,billing_instruct4
                             ,billing_instruct5
                             ,verification_date
                             ,pricing_id
                             ,dunning_letter_flag
                             ,denial_form_required
                             ,payor_type
                             ,payor_id
                             ,payor_addr_seq
                             ,include_flag
                             ,kit_type
                             ,kit_code
                             ,subscriber_country
                             ,subscriber_employer_country
                             ,edi_sub_id
                             ,ccgroup
                             ,pos_auto_send_flag
                             ,billing_seq
                             ,annual_limit
                             ,benefit_terms
                             ,pricing_guide
                             ,copay_type_flag
                             ,discount_type_flag
                             ,billing_type
                             ,copay
                             ,person_code
                             ,prior_auth_type
                             ,ncpdp_pt_address_flag
                             ,ncpdp_pregnancy_indicator_flag
                             ,ncpdp_cardholder_flag
                             ,ncpdp_group_id_flag
                             ,ncpdp_other_coverage_code
                             ,ncpdp_prescriber_name_flag
                             ,ncpdp_compound_code
                             ,ncpdp_unit_of_measure_code
                             ,ncpdp_patient_location
                             ,partnership_security_label
                             ,ncpdp_copay_flag
                             ,ncpdp_copay_porc
                             ,ncpdp_copay_payor_id
                             ,ncpdp_unit_dose_ind
                             ,ncpdp_cmpd_dsg_form_desc
                             ,ncpdp_cmpd_dspns_unit_form_ind
                             ,ncpdp_cmpd_route_admin
                             ,level_of_service
                             ,employer_id
                             ,subscriber_group_na
                             ,socsec_crypted
                             ,auto_send_270_flag
                             ,nvisit_type
                             ,do_not_autodial
                             ,patient_residence
                             ,pharm_service_type
                             ,ncpdp_prescriber_fname_flag
                             ,ncpdp_prescriber_address_flag
                             ,ncpdp_prescriber_phone_flag
                             ,ncpdp_prescriber_data_flag
                             ,ncpdp_cob_other_coverage_code   --[ CQ22863 Adding cob fields
                             ,ncpdp_cob_othr_payerid
                             ,ncpdp_cob_o_payer_ptr_amt_code
                             ,ncpdp_cob_othr_payer_covg_type
                             ,ncpdp_cob_cardholder_flag       --[ CQ22863
                             ,ncpdp_cob_group_id_flag         --CQ22967
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,update_date
                         FROM thot.patient_insurance)
               VALUES t_patient_insurance(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PATIENT_INSURANCE'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_patient_insurance.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT patient_id
                                   ,claim_center_no
                                   ,claim_center_seq
                                   ,effective_date
                                   ,expiration_date
                                   ,policy_no
                                   ,group_no
                                   ,authorization_no
                                   ,other
                                   ,subscriber_first
                                   ,subscriber_mi
                                   ,subscriber_last
                                   ,subscriber_generation
                                   ,subscriber_addr1
                                   ,subscriber_addr2
                                   ,subscriber_city
                                   ,subscriber_state
                                   ,subscriber_zip1
                                   ,subscriber_zip2
                                   ,subscriber_phone1
                                   ,subscriber_phone2
                                   ,subscriber_phone3
                                   ,subscriber_relation
                                   ,subscriber_socsec
                                   ,subscriber_date_of_birth
                                   ,subscriber_sex
                                   ,subscriber_employee_id
                                   ,subscriber_employer
                                   ,subscriber_employer_addr1
                                   ,subscriber_employer_addr2
                                   ,subscriber_employer_city
                                   ,subscriber_employer_state
                                   ,subscriber_employer_zip1
                                   ,subscriber_employer_zip2
                                   ,employer_phone1
                                   ,employer_phone2
                                   ,employer_phone3
                                   ,plan_type
                                   ,plan_name
                                   ,assignment_flag
                                   ,deductible_amt
                                   ,percent_paid
                                   ,max_out
                                   ,lifetime_limit
                                   ,medicare_suppl
                                   ,bill30_start_date
                                   ,discount_percent
                                   ,terms
                                   ,billing_instruct1
                                   ,billing_instruct2
                                   ,billing_instruct3
                                   ,billing_instruct4
                                   ,billing_instruct5
                                   ,verification_date
                                   ,pricing_id
                                   ,dunning_letter_flag
                                   ,denial_form_required
                                   ,payor_type
                                   ,payor_id
                                   ,payor_addr_seq
                                   ,include_flag
                                   ,kit_type
                                   ,kit_code
                                   ,subscriber_country
                                   ,subscriber_employer_country
                                   ,edi_sub_id
                                   ,ccgroup
                                   ,pos_auto_send_flag
                                   ,billing_seq
                                   ,annual_limit
                                   ,benefit_terms
                                   ,pricing_guide
                                   ,copay_type_flag
                                   ,discount_type_flag
                                   ,billing_type
                                   ,copay
                                   ,person_code
                                   ,prior_auth_type
                                   ,ncpdp_pt_address_flag
                                   ,ncpdp_pregnancy_indicator_flag
                                   ,ncpdp_cardholder_flag
                                   ,ncpdp_group_id_flag
                                   ,ncpdp_other_coverage_code
                                   ,ncpdp_prescriber_name_flag
                                   ,ncpdp_compound_code
                                   ,ncpdp_unit_of_measure_code
                                   ,ncpdp_patient_location
                                   ,partnership_security_label
                                   ,ncpdp_copay_flag
                                   ,ncpdp_copay_porc
                                   ,ncpdp_copay_payor_id
                                   ,ncpdp_unit_dose_ind
                                   ,ncpdp_cmpd_dsg_form_desc
                                   ,ncpdp_cmpd_dspns_unit_form_ind
                                   ,ncpdp_cmpd_route_admin
                                   ,level_of_service
                                   ,employer_id
                                   ,subscriber_group_na
                                   ,socsec_crypted
                                   ,auto_send_270_flag
                                   ,nvisit_type
                                   ,do_not_autodial
                                   ,patient_residence
                                   ,pharm_service_type
                                   ,ncpdp_prescriber_fname_flag
                                   ,ncpdp_prescriber_address_flag
                                   ,ncpdp_prescriber_phone_flag
                                   ,ncpdp_prescriber_data_flag
                                   ,ncpdp_cob_other_coverage_code   --[ CQ22863 Adding cob fields
                                   ,ncpdp_cob_othr_payerid
                                   ,ncpdp_cob_o_payer_ptr_amt_code
                                   ,ncpdp_cob_othr_payer_covg_type
                                   ,ncpdp_cob_cardholder_flag       --[ CQ22863 Adding cob fields
                                   ,ncpdp_cob_group_id_flag         --CQ22967
                                   ,created_by
                                   ,creation_date
                                   ,updated_by
                                   ,update_date
                               FROM thot.patient_insurance)
                     VALUES t_patient_insurance(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_patient_insurance(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               ,p_attr2_name    => 'CLAIM_CENTER_SEQ'
                                                               ,p_attr2_value   => t_patient_insurance(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).claim_center_seq
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_patient_insurance_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_patient_insurance.COUNT || ' | Records inserted:' ||(t_patient_insurance.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_patient_insurance.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.patient_insurance@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_patient_insurance_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE patient_insurance_cur;

    CLOSE patient_insurance_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF patient_insurance_cur%ISOPEN THEN
        CLOSE patient_insurance_cur;
      END IF;

      IF patient_insurance_rowid_cur%ISOPEN THEN
        CLOSE patient_insurance_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_patient_insurance;

--===================================================+
--| PULL_AUTH: Pulls auth table to rxhome.                                                      |
--===================================================+
  PROCEDURE pull_auth
  IS
    CURSOR auth_cur
    IS
      SELECT   cc_no
              ,auth_code
              ,patient_id
              ,partnership_security_label
          FROM rxh_stage.auth@rxh_stg atg
         WHERE processed_flag = 'N'
           AND atg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = atg.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.auth rxh
                           WHERE rxh.cc_no = atg.cc_no
                             AND rxh.auth_code = atg.auth_code
                             AND rxh.patient_id = atg.patient_id)
      ORDER BY ROWID;

    CURSOR auth_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.auth@rxh_stg atg
         WHERE processed_flag = 'N'
           AND atg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = atg.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.auth rxh
                           WHERE rxh.cc_no = atg.cc_no
                             AND rxh.auth_code = atg.auth_code
                             AND rxh.patient_id = atg.patient_id)
      ORDER BY ROWID;

    TYPE auth_tab IS TABLE OF auth_cur%ROWTYPE;

    TYPE auth_rowid_tab IS TABLE OF ROWID;

    t_auth        auth_tab;
    t_auth_rowid  auth_rowid_tab;
    t_triggers    triggers_tab;
    v_context     rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_AUTH';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'AUTH', t_triggers => t_triggers);

    OPEN auth_cur;

    OPEN auth_rowid_cur;

    LOOP
      FETCH auth_cur
      BULK COLLECT INTO t_auth LIMIT bulk_limit;

      EXIT WHEN t_auth.COUNT = 0;

      FETCH auth_rowid_cur
      BULK COLLECT INTO t_auth_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_auth.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT cc_no
                             ,auth_code
                             ,patient_id
                             ,partnership_security_label
                         FROM thot.auth)
               VALUES t_auth(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.AUTH'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_auth.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT cc_no
                                   ,auth_code
                                   ,patient_id
                                   ,partnership_security_label
                               FROM thot.auth)
                     VALUES t_auth(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_auth(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               ,p_attr2_name    => 'AUTH_CODE'
                                                               ,p_attr2_value   => t_auth(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).auth_code
                                                               ,p_attr3_name    => 'CC_NO'
                                                               ,p_attr3_value   => t_auth(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).cc_no
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_auth_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_auth.COUNT || ' | Records inserted:' ||(t_auth.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_auth.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.auth@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_auth_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE auth_cur;

    CLOSE auth_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF auth_cur%ISOPEN THEN
        CLOSE auth_cur;
      END IF;

      IF auth_rowid_cur%ISOPEN THEN
        CLOSE auth_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_auth;

--===================================================+
--| PULL_AUTH_DETAIL: Pulls auth_detail table to rxhome.                                |
--===================================================+
  PROCEDURE pull_auth_detail
  IS
    CURSOR auth_detail_cur
    IS
      SELECT   cc_no
              ,auth_code
              ,auth_seq
              ,effective_date
              ,expiration_date
              ,therapy_type
              ,therapy_type_days_auth
              ,therapy_type_days_used
              ,therapy_type_days_unused
              ,therapy_type_doses_auth
              ,therapy_type_doses_used
              ,therapy_type_doses_unused
              ,gdrug_abbrev
              ,drug_unit
              ,drug_units_auth
              ,drug_units_used
              ,drug_units_unused
              ,nvisit_type
              ,nvisits_auth
              ,nvisits_used
              ,nvisits_unused
              ,service_code
              ,nvisit_unit
              ,comments
              ,name_type
              ,name_id
              ,addr_seq
              ,phone_seq
              ,contact
              ,xdate
              ,xuser
              ,patient_id
              ,partnership_security_label
              ,auth_phonea
              ,auth_phoneb
              ,auth_phonec
              ,auth_ext
              ,auth_original_user
              ,auth_original_date
              ,prior_auth_type
              ,end_date_flag
              ,ndc_no
              ,inventory_id
              ,drug_type
              ,confirm_type
              ,confirm_type_others
              ,auth_obtained_from
              ,auth_obtained_phonea
              ,auth_obtained_phoneb
              ,auth_obtained_phonec
              ,auth_obtained_ext
              ,contact_info
              ,refills_auth
              ,refills_used
              ,refills_unused
              ,updated_by
              ,updated_date
              ,rx_match_date
          FROM rxh_stage.auth_detail@rxh_stg atg
         WHERE atg.processed_flag = 'N'
           AND atg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = atg.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.auth_detail rxh
                           WHERE rxh.cc_no = atg.cc_no
                             AND rxh.auth_code = atg.auth_code
                             AND rxh.patient_id = atg.patient_id)
      ORDER BY ROWID;

    CURSOR auth_detail_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.auth_detail@rxh_stg atg
         WHERE atg.processed_flag = 'N'
           AND atg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = atg.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.auth_detail rxh
                           WHERE rxh.cc_no = atg.cc_no
                             AND rxh.auth_code = atg.auth_code
                             AND rxh.patient_id = atg.patient_id)
      ORDER BY ROWID;

    TYPE auth_detail_tab IS TABLE OF auth_detail_cur%ROWTYPE;

    TYPE auth_detail_rowid_tab IS TABLE OF ROWID;

    t_auth_detail        auth_detail_tab;
    t_auth_detail_rowid  auth_detail_rowid_tab;
    t_triggers           triggers_tab;
    v_context            rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_AUTH_DETAIL';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'AUTH_DETAIL', t_triggers => t_triggers);

    OPEN auth_detail_cur;

    OPEN auth_detail_rowid_cur;

    LOOP
      FETCH auth_detail_cur
      BULK COLLECT INTO t_auth_detail LIMIT bulk_limit;

      EXIT WHEN t_auth_detail.COUNT = 0;

      FETCH auth_detail_rowid_cur
      BULK COLLECT INTO t_auth_detail_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_auth_detail.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT cc_no
                             ,auth_code
                             ,auth_seq
                             ,effective_date
                             ,expiration_date
                             ,therapy_type
                             ,therapy_type_days_auth
                             ,therapy_type_days_used
                             ,therapy_type_days_unused
                             ,therapy_type_doses_auth
                             ,therapy_type_doses_used
                             ,therapy_type_doses_unused
                             ,gdrug_abbrev
                             ,drug_unit
                             ,drug_units_auth
                             ,drug_units_used
                             ,drug_units_unused
                             ,nvisit_type
                             ,nvisits_auth
                             ,nvisits_used
                             ,nvisits_unused
                             ,service_code
                             ,nvisit_unit
                             ,comments
                             ,name_type
                             ,name_id
                             ,addr_seq
                             ,phone_seq
                             ,contact
                             ,xdate
                             ,xuser
                             ,patient_id
                             ,partnership_security_label
                             ,auth_phonea
                             ,auth_phoneb
                             ,auth_phonec
                             ,auth_ext
                             ,auth_original_user
                             ,auth_original_date
                             ,prior_auth_type
                             ,end_date_flag
                             ,ndc_no
                             ,inventory_id
                             ,drug_type
                             ,confirm_type
                             ,confirm_type_others
                             ,auth_obtained_from
                             ,auth_obtained_phonea
                             ,auth_obtained_phoneb
                             ,auth_obtained_phonec
                             ,auth_obtained_ext
                             ,contact_info
                             ,refills_auth
                             ,refills_used
                             ,refills_unused
                             ,updated_by
                             ,updated_date
                             ,rx_match_date
                         FROM thot.auth_detail)
               VALUES t_auth_detail(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.AUTH_DETAIL'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_auth_detail.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT cc_no
                                   ,auth_code
                                   ,auth_seq
                                   ,effective_date
                                   ,expiration_date
                                   ,therapy_type
                                   ,therapy_type_days_auth
                                   ,therapy_type_days_used
                                   ,therapy_type_days_unused
                                   ,therapy_type_doses_auth
                                   ,therapy_type_doses_used
                                   ,therapy_type_doses_unused
                                   ,gdrug_abbrev
                                   ,drug_unit
                                   ,drug_units_auth
                                   ,drug_units_used
                                   ,drug_units_unused
                                   ,nvisit_type
                                   ,nvisits_auth
                                   ,nvisits_used
                                   ,nvisits_unused
                                   ,service_code
                                   ,nvisit_unit
                                   ,comments
                                   ,name_type
                                   ,name_id
                                   ,addr_seq
                                   ,phone_seq
                                   ,contact
                                   ,xdate
                                   ,xuser
                                   ,patient_id
                                   ,partnership_security_label
                                   ,auth_phonea
                                   ,auth_phoneb
                                   ,auth_phonec
                                   ,auth_ext
                                   ,auth_original_user
                                   ,auth_original_date
                                   ,prior_auth_type
                                   ,end_date_flag
                                   ,ndc_no
                                   ,inventory_id
                                   ,drug_type
                                   ,confirm_type
                                   ,confirm_type_others
                                   ,auth_obtained_from
                                   ,auth_obtained_phonea
                                   ,auth_obtained_phoneb
                                   ,auth_obtained_phonec
                                   ,auth_obtained_ext
                                   ,contact_info
                                   ,refills_auth
                                   ,refills_used
                                   ,refills_unused
                                   ,updated_by
                                   ,updated_date
                                   ,rx_match_date
                               FROM thot.auth_detail)
                     VALUES t_auth_detail(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_auth_detail(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               ,p_attr2_name    => 'AUTH_CODE'
                                                               ,p_attr2_value   => t_auth_detail(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).auth_code
                                                               ,p_attr3_name    => 'CC_NO'
                                                               ,p_attr3_value   => t_auth_detail(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).cc_no
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_auth_detail_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_auth_detail.COUNT || ' | Records inserted:' ||(t_auth_detail.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_auth_detail.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.auth_detail@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_auth_detail_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE auth_detail_cur;

    CLOSE auth_detail_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF auth_detail_cur%ISOPEN THEN
        CLOSE auth_detail_cur;
      END IF;

      IF auth_detail_rowid_cur%ISOPEN THEN
        CLOSE auth_detail_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_auth_detail;

--=============================================+
--| PULL_IMS_DOCUMENTS: Pulls ims_documents table to rxhome.       |
--=============================================+
  PROCEDURE pull_ims_documents
  IS
    CURSOR ims_documents_cur
    IS
      SELECT   document_id
              ,doc_type_id
              ,doc_source_id
              ,patient_id
              ,date_scanned
              ,indexes_complete_flag
              ,total_pages
              ,doc_recd_date
              ,cs835_sent_date
              ,doc_sent_date
              ,ims_document_id
              ,batch_id
              ,partnership_security_label
              ,index_1_value
              ,index_2_value
              ,index_3_value
              ,index_4_value
              ,index_5_value
              ,index_6_value
              ,index_7_value
              ,index_8_value
              ,index_9_value
              ,index_10_value
              ,document_exception
              ,created_by
              ,creation_date
              ,updated_by
              ,update_date
              ,task_id
              ,source_image_id
              ,page_seq
              ,page_type
              ,old_doc_type_id
              ,reindex_status
              ,rescanned_date
              ,image_active_status
              ,index_11_value
              ,index_12_value
              ,index_13_value
              ,index_14_value
              ,index_15_value
              ,workorder_guid
              ,batch_name
              ,comm_item_guid
              ,comm_item_seq
              ,doc_type_abbr
              ,old_doc_type_abbr
              ,medco_batch_id
              ,document_category_code
              ,index_16_value
              ,index_17_value
              ,index_18_value
              ,index_19_value
              ,index_20_value
              ,index_21_value
              ,index_22_value
              ,index_23_value
              ,index_24_value
              ,index_25_value
              ,filenet_doc_id
          FROM rxh_stage.ims_documents@rxh_stg doc
         WHERE doc.processed_flag = 'N'
           AND doc.ready_flag = 'Y'
           AND doc.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = doc.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM doc_owner.ims_documents rxh
                           WHERE rxh.document_id = doc.document_id)
      ORDER BY ROWID;

    CURSOR ims_documents_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.ims_documents@rxh_stg doc
         WHERE doc.processed_flag = 'N'
           AND doc.ready_flag = 'Y'
           AND doc.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                       WHERE rxh.ID = doc.patient_id)
           AND NOT EXISTS(SELECT 1
                            FROM doc_owner.ims_documents rxh
                           WHERE rxh.document_id = doc.document_id)
      ORDER BY ROWID;

    TYPE ims_documents_tab IS TABLE OF ims_documents_cur%ROWTYPE;

    TYPE ims_documents_rowid_tab IS TABLE OF ROWID;

    t_ims_documents        ims_documents_tab;
    t_ims_documents_rowid  ims_documents_rowid_tab;
    t_triggers             triggers_tab;
    v_context              rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_IMS_DOCUMENTS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'DOC_OWNER', p_table_name => 'IMS_DOCUMENTS', t_triggers => t_triggers);

    OPEN ims_documents_cur;

    OPEN ims_documents_rowid_cur;

    LOOP
      FETCH ims_documents_cur
      BULK COLLECT INTO t_ims_documents LIMIT bulk_limit;

      EXIT WHEN t_ims_documents.COUNT = 0;

      FETCH ims_documents_rowid_cur
      BULK COLLECT INTO t_ims_documents_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_ims_documents.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT document_id
                             ,doc_type_id
                             ,doc_source_id
                             ,patient_id
                             ,date_scanned
                             ,indexes_complete_flag
                             ,total_pages
                             ,doc_recd_date
                             ,cs835_sent_date
                             ,doc_sent_date
                             ,ims_document_id
                             ,batch_id
                             ,partnership_security_label
                             ,index_1_value
                             ,index_2_value
                             ,index_3_value
                             ,index_4_value
                             ,index_5_value
                             ,index_6_value
                             ,index_7_value
                             ,index_8_value
                             ,index_9_value
                             ,index_10_value
                             ,document_exception
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,update_date
                             ,task_id
                             ,source_image_id
                             ,page_seq
                             ,page_type
                             ,old_doc_type_id
                             ,reindex_status
                             ,rescanned_date
                             ,image_active_status
                             ,index_11_value
                             ,index_12_value
                             ,index_13_value
                             ,index_14_value
                             ,index_15_value
                             ,workorder_guid
                             ,batch_name
                             ,comm_item_guid
                             ,comm_item_seq
                             ,doc_type_abbr
                             ,old_doc_type_abbr
                             ,medco_batch_id
                             ,document_category_code
                             ,index_16_value
                             ,index_17_value
                             ,index_18_value
                             ,index_19_value
                             ,index_20_value
                             ,index_21_value
                             ,index_22_value
                             ,index_23_value
                             ,index_24_value
                             ,index_25_value
                             ,filenet_doc_id
                         FROM doc_owner.ims_documents)
               VALUES t_ims_documents(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'DOC_OWNER.IMS_DOCUMENTS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_ims_documents.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT document_id
                                   ,doc_type_id
                                   ,doc_source_id
                                   ,patient_id
                                   ,date_scanned
                                   ,indexes_complete_flag
                                   ,total_pages
                                   ,doc_recd_date
                                   ,cs835_sent_date
                                   ,doc_sent_date
                                   ,ims_document_id
                                   ,batch_id
                                   ,partnership_security_label
                                   ,index_1_value
                                   ,index_2_value
                                   ,index_3_value
                                   ,index_4_value
                                   ,index_5_value
                                   ,index_6_value
                                   ,index_7_value
                                   ,index_8_value
                                   ,index_9_value
                                   ,index_10_value
                                   ,document_exception
                                   ,created_by
                                   ,creation_date
                                   ,updated_by
                                   ,update_date
                                   ,task_id
                                   ,source_image_id
                                   ,page_seq
                                   ,page_type
                                   ,old_doc_type_id
                                   ,reindex_status
                                   ,rescanned_date
                                   ,image_active_status
                                   ,index_11_value
                                   ,index_12_value
                                   ,index_13_value
                                   ,index_14_value
                                   ,index_15_value
                                   ,workorder_guid
                                   ,batch_name
                                   ,comm_item_guid
                                   ,comm_item_seq
                                   ,doc_type_abbr
                                   ,old_doc_type_abbr
                                   ,medco_batch_id
                                   ,document_category_code
                                   ,index_16_value
                                   ,index_17_value
                                   ,index_18_value
                                   ,index_19_value
                                   ,index_20_value
                                   ,index_21_value
                                   ,index_22_value
                                   ,index_23_value
                                   ,index_24_value
                                   ,index_25_value
                                   ,filenet_doc_id
                               FROM doc_owner.ims_documents)
                     VALUES t_ims_documents(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'DOCUMENT_ID'
                                                               ,p_attr1_value   => t_ims_documents(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).document_id
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_ims_documents_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_ims_documents.COUNT || ' | Records inserted:' ||(t_ims_documents.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_ims_documents.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.ims_documents@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_ims_documents_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE ims_documents_cur;

    CLOSE ims_documents_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF ims_documents_cur%ISOPEN THEN
        CLOSE ims_documents_cur;
      END IF;

      IF ims_documents_rowid_cur%ISOPEN THEN
        CLOSE ims_documents_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_ims_documents;

--=============================================+
--| PULL_PHY_ADDRESSES: Pulls ADDRESSES table to rxhome.           |
--=============================================+
  PROCEDURE pull_phy_addresses
  IS
    CURSOR addresses_cur
    IS
      SELECT   name_type
              ,name_id
              ,addr_seq
              ,attn
              ,NAME
              ,addr1
              ,addr2
              ,city
              ,state
              ,zip
              ,comments
              ,address_type
              ,country
              ,status
              ,county
              ,partnership_security_label
              ,company_name
              ,contact_first_name
              ,contact_last_name
              ,phy_clinic_flag
              ,marketing_id
              ,addr_effective_date
              ,created_by
              ,creation_date
              ,updated_by
              ,updated_date
              ,scc_address_flag
              ,code1_status
              ,code1_value
          FROM rxh_stage.addresses@rxh_stg atg
         WHERE atg.processed_flag = 'N'
           AND atg.name_type = 'D'
           AND atg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.physicians phy
                       WHERE phy.ID = atg.name_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.addresses rxh
                           WHERE rxh.name_type = atg.name_type
                             AND rxh.name_id = atg.name_id
                             AND rxh.addr_seq = atg.addr_seq)
      ORDER BY ROWID;

    CURSOR addresses_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.addresses@rxh_stg atg
         WHERE atg.processed_flag = 'N'
           AND atg.name_type = 'D'
           AND atg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.physicians phy
                       WHERE phy.ID = atg.name_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.addresses rxh
                           WHERE rxh.name_type = atg.name_type
                             AND rxh.name_id = atg.name_id
                             AND rxh.addr_seq = atg.addr_seq)
      ORDER BY ROWID;

    TYPE addresses_tab IS TABLE OF addresses_cur%ROWTYPE;

    TYPE addresses_rowid_tab IS TABLE OF ROWID;

    t_addresses        addresses_tab;
    t_addresses_rowid  addresses_rowid_tab;
    t_triggers         triggers_tab;
    v_context          rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PHY_ADDRESSES';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'ADDRESSES', t_triggers => t_triggers);

    OPEN addresses_cur;

    OPEN addresses_rowid_cur;

    LOOP
      FETCH addresses_cur
      BULK COLLECT INTO t_addresses LIMIT bulk_limit;

      EXIT WHEN t_addresses.COUNT = 0;

      FETCH addresses_rowid_cur
      BULK COLLECT INTO t_addresses_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_addresses.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT name_type
                             ,name_id
                             ,addr_seq
                             ,attn
                             ,NAME
                             ,addr1
                             ,addr2
                             ,city
                             ,state
                             ,zip
                             ,comments
                             ,address_type
                             ,country
                             ,status
                             ,county
                             ,partnership_security_label
                             ,company_name
                             ,contact_first_name
                             ,contact_last_name
                             ,phy_clinic_flag
                             ,marketing_id
                             ,addr_effective_date
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,updated_date
                             ,scc_address_flag
                             ,code1_status
                             ,code1_value
                         FROM thot.addresses)
               VALUES t_addresses(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.ADDRESSES'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_addresses.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT name_type
                                   ,name_id
                                   ,addr_seq
                                   ,attn
                                   ,NAME
                                   ,addr1
                                   ,addr2
                                   ,city
                                   ,state
                                   ,zip
                                   ,comments
                                   ,address_type
                                   ,country
                                   ,status
                                   ,county
                                   ,partnership_security_label
                                   ,company_name
                                   ,contact_first_name
                                   ,contact_last_name
                                   ,phy_clinic_flag
                                   ,marketing_id
                                   ,addr_effective_date
                                   ,created_by
                                   ,creation_date
                                   ,updated_by
                                   ,updated_date
                                   ,scc_address_flag
                                   ,code1_status
                                   ,code1_value
                               FROM thot.addresses)
                     VALUES t_addresses(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'PHYSICIAN_ID'
                                                               ,p_attr1_value   => t_addresses(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).name_id
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_addresses_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_addresses.COUNT || ' | Records inserted:' ||(t_addresses.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_addresses.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.addresses@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_addresses_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE addresses_cur;

    CLOSE addresses_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF addresses_cur%ISOPEN THEN
        CLOSE addresses_cur;
      END IF;

      IF addresses_rowid_cur%ISOPEN THEN
        CLOSE addresses_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_phy_addresses;

--=======================================+
--| PULL_PHY_PHONES: Pulls PHONES table to rxhome.           |
--=======================================+
  PROCEDURE pull_phy_phones
  IS
    CURSOR phones_cur
    IS
      SELECT   name_type
              ,name_id
              ,phone_seq
              ,phone_type
              ,phonea
              ,phoneb
              ,phonec
              ,phone_ext
              ,phone_comment
              ,addr_seq
              ,partnership_security_label
              ,contact_flag
              ,created_by
              ,creation_date
              ,updated_by
              ,updated_date
              ,call_day
              ,call_time
              ,call_time_zone
          FROM rxh_stage.phones@rxh_stg ptg
         WHERE ptg.processed_flag = 'N'
           AND ptg.name_type = 'D'
           AND ptg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.physicians phy
                       WHERE phy.ID = ptg.name_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.phones rxh
                           WHERE rxh.name_type = ptg.name_type
                             AND rxh.name_id = ptg.name_id
                             AND rxh.addr_seq = ptg.addr_seq
                             AND rxh.phone_seq = ptg.phone_seq)
      ORDER BY ROWID;

    CURSOR phones_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.phones@rxh_stg ptg
         WHERE ptg.processed_flag = 'N'
           AND ptg.name_type = 'D'
           AND ptg.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.physicians phy
                       WHERE phy.ID = ptg.name_id)
           AND NOT EXISTS(SELECT 1
                            FROM thot.phones rxh
                           WHERE rxh.name_type = ptg.name_type
                             AND rxh.name_id = ptg.name_id
                             AND rxh.addr_seq = ptg.addr_seq
                             AND rxh.phone_seq = ptg.phone_seq)
      ORDER BY ROWID;

    TYPE phones_tab IS TABLE OF phones_cur%ROWTYPE;

    TYPE phones_rowid_tab IS TABLE OF ROWID;

    t_phones        phones_tab;
    t_phones_rowid  phones_rowid_tab;
    t_triggers      triggers_tab;
    v_context       rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PHY_PHONES';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PHONES', t_triggers => t_triggers);

    OPEN phones_cur;

    OPEN phones_rowid_cur;

    LOOP
      FETCH phones_cur
      BULK COLLECT INTO t_phones LIMIT bulk_limit;

      EXIT WHEN t_phones.COUNT = 0;

      FETCH phones_rowid_cur
      BULK COLLECT INTO t_phones_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_phones.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT name_type
                             ,name_id
                             ,phone_seq
                             ,phone_type
                             ,phonea
                             ,phoneb
                             ,phonec
                             ,phone_ext
                             ,phone_comment
                             ,addr_seq
                             ,partnership_security_label
                             ,contact_flag
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,updated_date
                             ,call_day
                             ,call_time
                             ,call_time_zone
                         FROM thot.phones)
               VALUES t_phones(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PHONES'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_phones.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT name_type
                                   ,name_id
                                   ,phone_seq
                                   ,phone_type
                                   ,phonea
                                   ,phoneb
                                   ,phonec
                                   ,phone_ext
                                   ,phone_comment
                                   ,addr_seq
                                   ,partnership_security_label
                                   ,contact_flag
                                   ,created_by
                                   ,creation_date
                                   ,updated_by
                                   ,updated_date
                                   ,call_day
                                   ,call_time
                                   ,call_time_zone
                               FROM thot.phones)
                     VALUES t_phones(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'PHYSICIAN_ID'
                                                               ,p_attr1_value   => t_phones(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).name_id
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_phones_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_phones.COUNT || ' | Records inserted:' ||(t_phones.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_phones.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.phones@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_phones_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE phones_cur;

    CLOSE phones_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF phones_cur%ISOPEN THEN
        CLOSE phones_cur;
      END IF;

      IF phones_rowid_cur%ISOPEN THEN
        CLOSE phones_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_phy_phones;

--=============================================+
--| PULL_A2R_USERS_XREF: Pulls a2r_users_xref table to rxhome.      |
--=============================================+
  PROCEDURE pull_a2r_users_xref
  IS
    CURSOR a2r_users_xref_cur
    IS
      SELECT   atlas_user
              ,rxhome_user
              ,full_name
              ,atlas_initials
              ,rxhome_initials
              ,pharmacist
              ,technician
          FROM rxh_stage.a2r_users_xref@rxh_stg usr
         WHERE usr.processed_flag = 'N'
           AND NOT EXISTS(SELECT 1
                            FROM thot.a2r_users_xref rxh
                           WHERE rxh.atlas_user = usr.atlas_user)
      ORDER BY ROWID;

    CURSOR a2r_users_xref_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.a2r_users_xref@rxh_stg usr
         WHERE usr.processed_flag = 'N'
           AND NOT EXISTS(SELECT 1
                            FROM thot.a2r_users_xref rxh
                           WHERE rxh.atlas_user = usr.atlas_user)
      ORDER BY ROWID;

    TYPE a2r_users_xref_tab IS TABLE OF a2r_users_xref_cur%ROWTYPE;

    TYPE a2r_users_xref_rowid_tab IS TABLE OF ROWID;

    t_a2r_users_xref        a2r_users_xref_tab;
    t_a2r_users_xref_rowid  a2r_users_xref_rowid_tab;
    t_triggers              triggers_tab;
    v_context               rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_A2R_USERS_XREF';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'A2R_USERS_XREF', t_triggers => t_triggers);

    OPEN a2r_users_xref_cur;

    OPEN a2r_users_xref_rowid_cur;

    LOOP
      FETCH a2r_users_xref_cur
      BULK COLLECT INTO t_a2r_users_xref LIMIT bulk_limit;

      EXIT WHEN t_a2r_users_xref.COUNT = 0;

      FETCH a2r_users_xref_rowid_cur
      BULK COLLECT INTO t_a2r_users_xref_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_a2r_users_xref.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT atlas_user
                             ,rxhome_user
                             ,full_name
                             ,atlas_initials
                             ,rxhome_initials
                             ,pharmacist
                             ,technician
                         FROM thot.a2r_users_xref)
               VALUES t_a2r_users_xref(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.A2R_USERS_XREF'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_a2r_users_xref.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT atlas_user
                                   ,rxhome_user
                                   ,full_name
                                   ,atlas_initials
                                   ,rxhome_initials
                                   ,pharmacist
                                   ,technician
                               FROM thot.a2r_users_xref)
                     VALUES t_a2r_users_xref(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'ATLAS_USER'
                                                               ,p_attr1_value   => t_a2r_users_xref(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).atlas_user
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_a2r_users_xref_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_a2r_users_xref.COUNT || ' | Records inserted:' ||(t_a2r_users_xref.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_a2r_users_xref.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.a2r_users_xref@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_a2r_users_xref_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE a2r_users_xref_cur;

    CLOSE a2r_users_xref_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF a2r_users_xref_cur%ISOPEN THEN
        CLOSE a2r_users_xref_cur;
      END IF;

      IF a2r_users_xref_rowid_cur%ISOPEN THEN
        CLOSE a2r_users_xref_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_a2r_users_xref;

--=============================================+
--| PULL_USERS: Pulls users table to rxhome.                                     |
--=============================================+
  PROCEDURE pull_users
  IS
    CURSOR users_cur
    IS
      SELECT   xuser
              ,user_name
              ,svcbr_id
              ,default_commnote_type
              ,ms_disclaimer_view_date
              ,medispan_flag
              ,active_date
              ,inactive_date
              ,password_changed_date
              ,last_login
              ,report_server_id
              ,file_server_id
              ,team
              ,email
              ,label_transition
              ,ROLE
              ,division
              ,win_id
              ,mobile_user
              ,jphone
              ,hap_enabled
              ,restrict_db_access
              ,user_type
          FROM rxh_stage.users@rxh_stg usr
         WHERE usr.processed_flag = 'N'
           AND usr.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.users rxh
                           WHERE rxh.xuser = usr.xuser)
      ORDER BY ROWID;

    CURSOR users_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.users@rxh_stg usr
         WHERE usr.processed_flag = 'N'
           AND usr.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.users rxh
                           WHERE rxh.xuser = usr.xuser)
      ORDER BY ROWID;

    TYPE users_tab IS TABLE OF users_cur%ROWTYPE;

    TYPE users_rowid_tab IS TABLE OF ROWID;

    t_users        users_tab;
    t_users_rowid  users_rowid_tab;
    t_triggers     triggers_tab;
    v_context      rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_USERS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'USERS', t_triggers => t_triggers);

    OPEN users_cur;

    OPEN users_rowid_cur;

    LOOP
      FETCH users_cur
      BULK COLLECT INTO t_users LIMIT bulk_limit;

      EXIT WHEN t_users.COUNT = 0;

      FETCH users_rowid_cur
      BULK COLLECT INTO t_users_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_users.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT xuser
                             ,user_name
                             ,svcbr_id
                             ,default_commnote_type
                             ,ms_disclaimer_view_date
                             ,medispan_flag
                             ,active_date
                             ,inactive_date
                             ,password_changed_date
                             ,last_login
                             ,report_server_id
                             ,file_server_id
                             ,team
                             ,email
                             ,label_transition
                             ,ROLE
                             ,division
                             ,win_id
                             ,mobile_user
                             ,jphone
                             ,hap_enabled
                             ,restrict_db_access
                             ,user_type
                         FROM thot.users)
               VALUES t_users(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.USERS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_users.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT xuser
                                   ,user_name
                                   ,svcbr_id
                                   ,default_commnote_type
                                   ,ms_disclaimer_view_date
                                   ,medispan_flag
                                   ,active_date
                                   ,inactive_date
                                   ,password_changed_date
                                   ,last_login
                                   ,report_server_id
                                   ,file_server_id
                                   ,team
                                   ,email
                                   ,label_transition
                                   ,ROLE
                                   ,division
                                   ,win_id
                                   ,mobile_user
                                   ,jphone
                                   ,hap_enabled
                                   ,restrict_db_access
                                   ,user_type
                               FROM thot.users)
                     VALUES t_users(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'XUSER'
                                                               ,p_attr1_value   => t_users(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).xuser
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_users_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_users.COUNT || ' | Records inserted:' ||(t_users.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_users.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.users@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_users_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE users_cur;

    CLOSE users_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF users_cur%ISOPEN THEN
        CLOSE users_cur;
      END IF;

      IF users_rowid_cur%ISOPEN THEN
        CLOSE users_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_users;

--=============================================+
--| PULL_PHARMACISTS: Pulls pharmacists table to rxhome.                 |
--=============================================+
  PROCEDURE pull_pharmacists
  IS
    CURSOR pharmacists_cur
    IS
      SELECT   ID
              ,LAST
              ,FIRST
              ,mi
              ,mon_of_birth
              ,day_of_birth
              ,yr_of_birth
              ,initials
              ,xuser
              ,addr1
              ,addr2
              ,city
              ,state
              ,zip1
              ,zip2
              ,country
              ,comments
              ,socsec
              ,employment_start_date
              ,employment_stop_date
              ,invoicer
              ,marketing_id
              ,pricing_id
              ,pac_disp_rph
              ,npi
          FROM rxh_stage.pharmacists@rxh_stg ph
         WHERE ph.processed_flag = 'N'
           AND ph.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.users rxh
                       WHERE rxh.xuser = ph.xuser)
           AND NOT EXISTS(SELECT 1
                            FROM thot.pharmacists rxh
                           WHERE rxh.initials = ph.initials
                              OR rxh.xuser = ph.xuser)
      ORDER BY ROWID;

    CURSOR pharmacists_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.pharmacists@rxh_stg ph
         WHERE ph.processed_flag = 'N'
           AND ph.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.users rxh
                       WHERE rxh.xuser = ph.xuser)
           AND NOT EXISTS(SELECT 1
                            FROM thot.pharmacists rxh
                           WHERE rxh.initials = ph.initials
                              OR rxh.xuser = ph.xuser)
      ORDER BY ROWID;

    TYPE pharmacists_tab IS TABLE OF pharmacists_cur%ROWTYPE;

    TYPE pharmacists_rowid_tab IS TABLE OF ROWID;

    t_pharmacists        pharmacists_tab;
    t_pharmacists_rowid  pharmacists_rowid_tab;
    t_triggers           triggers_tab;
    v_context            rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PHARMACISTS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PHARMACISTS', t_triggers => t_triggers);

    OPEN pharmacists_cur;

    OPEN pharmacists_rowid_cur;

    LOOP
      FETCH pharmacists_cur
      BULK COLLECT INTO t_pharmacists LIMIT bulk_limit;

      EXIT WHEN t_pharmacists.COUNT = 0;

      FETCH pharmacists_rowid_cur
      BULK COLLECT INTO t_pharmacists_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_pharmacists.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT ID
                             ,LAST
                             ,FIRST
                             ,mi
                             ,mon_of_birth
                             ,day_of_birth
                             ,yr_of_birth
                             ,initials
                             ,xuser
                             ,addr1
                             ,addr2
                             ,city
                             ,state
                             ,zip1
                             ,zip2
                             ,country
                             ,comments
                             ,socsec
                             ,employment_start_date
                             ,employment_stop_date
                             ,invoicer
                             ,marketing_id
                             ,pricing_id
                             ,pac_disp_rph
                             ,npi
                         FROM thot.pharmacists)
               VALUES t_pharmacists(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PHARMACISTS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_pharmacists.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT ID
                             ,LAST
                             ,FIRST
                             ,mi
                             ,mon_of_birth
                             ,day_of_birth
                             ,yr_of_birth
                             ,initials
                             ,xuser
                             ,addr1
                             ,addr2
                             ,city
                             ,state
                             ,zip1
                             ,zip2
                             ,country
                             ,comments
                             ,socsec
                             ,employment_start_date
                             ,employment_stop_date
                             ,invoicer
                             ,marketing_id
                             ,pricing_id
                             ,pac_disp_rph
                             ,npi
                         FROM thot.pharmacists)
               VALUES t_pharmacists(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'INITIALS'
                                                               ,p_attr1_value   => t_pharmacists(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).initials
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_pharmacists_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_pharmacists.COUNT || ' | Records inserted:' ||(t_pharmacists.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_pharmacists.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.pharmacists@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_pharmacists_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE pharmacists_cur;

    CLOSE pharmacists_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF pharmacists_cur%ISOPEN THEN
        CLOSE pharmacists_cur;
      END IF;

      IF pharmacists_rowid_cur%ISOPEN THEN
        CLOSE pharmacists_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_pharmacists;

--=============================================+
--| PULL_TECHNICIANS: Pulls technicians table to rxhome.                    |
--=============================================+
  PROCEDURE pull_technicians
  IS
    CURSOR technicians_cur
    IS
      SELECT   ID
              ,LAST
              ,FIRST
              ,mi
              ,mon_of_birth
              ,day_of_birth
              ,yr_of_birth
              ,initials
              ,addr1
              ,addr2
              ,city
              ,state
              ,zip1
              ,zip2
              ,country
              ,comments
              ,socsec
              ,employment_start_date
              ,employment_stop_date
              ,xuser
              ,invoicer
              ,marketing_id
              ,pricing_id
          FROM rxh_stage.technicians@rxh_stg tch
         WHERE tch.processed_flag = 'N'
           AND tch.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.users rxh
                       WHERE rxh.xuser = tch.xuser)
           AND NOT EXISTS(SELECT 1
                            FROM thot.technicians rxh
                           WHERE rxh.initials = tch.initials
                              OR rxh.xuser = tch.xuser)
      ORDER BY ROWID;

    CURSOR technicians_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.technicians@rxh_stg tch
         WHERE tch.processed_flag = 'N'
           AND tch.run_id = g_run_id
           AND EXISTS(SELECT 1
                        FROM thot.users rxh
                       WHERE rxh.xuser = tch.xuser)
           AND NOT EXISTS(SELECT 1
                            FROM thot.technicians rxh
                           WHERE rxh.initials = tch.initials
                              OR rxh.xuser = tch.xuser)
      ORDER BY ROWID;

    TYPE technicians_tab IS TABLE OF technicians_cur%ROWTYPE;

    TYPE technicians_rowid_tab IS TABLE OF ROWID;

    t_technicians        technicians_tab;
    t_technicians_rowid  technicians_rowid_tab;
    t_triggers           triggers_tab;
    v_context            rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_TECHNICIANS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'TECHNICIANS', t_triggers => t_triggers);

    OPEN technicians_cur;

    OPEN technicians_rowid_cur;

    LOOP
      FETCH technicians_cur
      BULK COLLECT INTO t_technicians LIMIT bulk_limit;

      EXIT WHEN t_technicians.COUNT = 0;

      FETCH technicians_rowid_cur
      BULK COLLECT INTO t_technicians_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_technicians.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT ID
                             ,LAST
                             ,FIRST
                             ,mi
                             ,mon_of_birth
                             ,day_of_birth
                             ,yr_of_birth
                             ,initials
                             ,addr1
                             ,addr2
                             ,city
                             ,state
                             ,zip1
                             ,zip2
                             ,country
                             ,comments
                             ,socsec
                             ,employment_start_date
                             ,employment_stop_date
                             ,xuser
                             ,invoicer
                             ,marketing_id
                             ,pricing_id
                         FROM thot.technicians)
               VALUES t_technicians(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.TECHNICIANS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_technicians.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT ID
                             ,LAST
                             ,FIRST
                             ,mi
                             ,mon_of_birth
                             ,day_of_birth
                             ,yr_of_birth
                             ,initials
                             ,addr1
                             ,addr2
                             ,city
                             ,state
                             ,zip1
                             ,zip2
                             ,country
                             ,comments
                             ,socsec
                             ,employment_start_date
                             ,employment_stop_date
                             ,xuser
                             ,invoicer
                             ,marketing_id
                             ,pricing_id
                         FROM thot.technicians)
               VALUES t_technicians(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'INITIALS'
                                                               ,p_attr1_value   => t_technicians(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).xuser
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_technicians_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_technicians.COUNT || ' | Records inserted:' ||(t_technicians.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_technicians.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.technicians@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_technicians_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE technicians_cur;

    CLOSE technicians_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF technicians_cur%ISOPEN THEN
        CLOSE technicians_cur;
      END IF;

      IF technicians_rowid_cur%ISOPEN THEN
        CLOSE technicians_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_technicians;

--================================================+
--| PULL_USER_FLAG_GROUPS: Pulls user_flag_groups table to rxhome.      |
--================================================+
  PROCEDURE pull_user_flag_groups
  IS
    CURSOR user_flag_groups_cur
    IS
      SELECT group_code
                  ,clinical_flag
      FROM     rxh_stage.user_flag_groups@rxh_stg usr
         WHERE usr.processed_flag = 'N'
           AND usr.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.user_flag_groups rxh
                           WHERE rxh.group_code = usr.group_code)
      ORDER BY ROWID;

    CURSOR user_flag_groups_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.user_flag_groups@rxh_stg usr
         WHERE usr.processed_flag = 'N'
           AND usr.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.user_flag_groups rxh
                           WHERE rxh.group_code = usr.group_code)
      ORDER BY ROWID;

    TYPE user_flag_groups_tab IS TABLE OF user_flag_groups_cur%ROWTYPE;

    TYPE user_flag_groups_rowid_tab IS TABLE OF ROWID;

    t_user_flag_groups        user_flag_groups_tab;
    t_user_flag_groups_rowid  user_flag_groups_rowid_tab;
    t_triggers                triggers_tab;
    v_context                 rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_USER_FLAG_GROUPS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'USER_FLAG_GROUPS', t_triggers => t_triggers);

    OPEN user_flag_groups_cur;

    OPEN user_flag_groups_rowid_cur;

    LOOP
      FETCH user_flag_groups_cur
      BULK COLLECT INTO t_user_flag_groups LIMIT bulk_limit;

      EXIT WHEN t_user_flag_groups.COUNT = 0;

      FETCH user_flag_groups_rowid_cur
      BULK COLLECT INTO t_user_flag_groups_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_user_flag_groups.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT group_code
                                             ,clinical_flag
                       FROM   thot.user_flag_groups)
               VALUES t_user_flag_groups(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.USER_FLAG_GROUPS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_user_flag_groups.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT group_code
                                   ,clinical_flag
                               FROM thot.user_flag_groups)
                       VALUES t_user_flag_groups(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'GROUP_CODE'
                                                               ,p_attr1_value   => t_user_flag_groups(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).group_code
                                                               ,p_attr2_name    => 'CLINICAL_FLAG'
                                                               ,p_attr2_value    => t_user_flag_groups(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).CLINICAL_FLAG
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_user_flag_groups_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_user_flag_groups.COUNT || ' | Records inserted:' ||(t_user_flag_groups.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_user_flag_groups.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.user_flag_groups@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_user_flag_groups_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE user_flag_groups_cur;

    CLOSE user_flag_groups_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF user_flag_groups_cur%ISOPEN THEN
        CLOSE user_flag_groups_cur;
      END IF;

      IF user_flag_groups_rowid_cur%ISOPEN THEN
        CLOSE user_flag_groups_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_user_flag_groups;

--================================================+
--| PULL_USER_FLAG_TYPES: Pulls user_flag_types table to rxhome.           |
--================================================+
  PROCEDURE pull_user_flag_types
  IS
    CURSOR user_flag_types_cur
    IS
      SELECT   user_flag_type
                    ,group_code
                    ,description
                    ,status
                    ,priority
                    ,therapy_type
                    ,therapy_class
                    ,required_flag
          FROM rxh_stage.user_flag_types@rxh_stg usr
         WHERE usr.processed_flag = 'N'
           AND usr.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.user_flag_types rxh
                           WHERE rxh.group_code = usr.group_code
                             AND rxh.user_flag_type = usr.user_flag_type)
      ORDER BY ROWID;

    CURSOR user_flag_types_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.user_flag_types@rxh_stg usr
         WHERE usr.processed_flag = 'N'
           AND usr.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.user_flag_types rxh
                           WHERE rxh.group_code = usr.group_code
                             AND rxh.user_flag_type = usr.user_flag_type)
      ORDER BY ROWID;

    TYPE user_flag_types_tab IS TABLE OF user_flag_types_cur%ROWTYPE;

    TYPE user_flag_types_rowid_tab IS TABLE OF ROWID;

    t_user_flag_types        user_flag_types_tab;
    t_user_flag_types_rowid  user_flag_types_rowid_tab;
    t_triggers               triggers_tab;
    v_context                rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_USER_FLAG_TYPES';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'USER_FLAG_TYPES', t_triggers => t_triggers);

    OPEN user_flag_types_cur;

    OPEN user_flag_types_rowid_cur;

    LOOP
      FETCH user_flag_types_cur
      BULK COLLECT INTO t_user_flag_types LIMIT bulk_limit;

      EXIT WHEN t_user_flag_types.COUNT = 0;

      FETCH user_flag_types_rowid_cur
      BULK COLLECT INTO t_user_flag_types_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_user_flag_types.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT user_flag_type
                                              ,group_code
                                              ,description
                                              ,status
                                              ,priority
                                              ,therapy_type
                                              ,therapy_class
                                              ,required_flag
                         FROM thot.user_flag_types)
               VALUES t_user_flag_types(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.USER_FLAG_TYPES'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_user_flag_types.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT user_flag_type
                                    ,group_code
                                    ,description
                                    ,status
                                    ,priority
                                    ,therapy_type
                                    ,therapy_class
                                    ,required_flag
                               FROM thot.user_flag_types)
                     VALUES t_user_flag_types(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'USER_FLAG_TYPE'
                                                               ,p_attr1_value   => t_user_flag_types(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).user_flag_type
                                                               ,p_attr2_name    => 'GROUP_CODE'
                                                               ,p_attr2_value   => t_user_flag_types(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).group_code
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_user_flag_types_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_user_flag_types.COUNT || ' | Records inserted:' ||(t_user_flag_types.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_user_flag_types.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.user_flag_types@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_user_flag_types_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE user_flag_types_cur;

    CLOSE user_flag_types_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF user_flag_types_cur%ISOPEN THEN
        CLOSE user_flag_types_cur;
      END IF;

      IF user_flag_types_rowid_cur%ISOPEN THEN
        CLOSE user_flag_types_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_user_flag_types;

--================================================+
--| PULL_PT_USER_FLAG_TYPES: Pulls pt_user_flag_types table to rxhome. |
--================================================+
  PROCEDURE pull_pt_user_flag_types
  IS
    CURSOR pt_user_flag_types_cur
    IS
      SELECT   patient_id
              ,user_flag_type
              ,group_code
              ,start_date
              ,stop_date
              ,comments
              ,partnership_security_label
              ,created_by
              ,creation_date
              ,updated_by
              ,update_date
          FROM rxh_stage.pt_user_flag_types@rxh_stg usr
         WHERE usr.processed_flag = 'N'
           AND usr.run_id = g_run_id
          AND EXISTS(SELECT 1
                              FROM thot.patients_table rxh
                              WHERE rxh.id = usr.patient_id)
      ORDER BY ROWID;

    CURSOR pt_user_flag_types_rowid_cur
    IS
      SELECT   ROWID row_id
      FROM rxh_stage.pt_user_flag_types@rxh_stg usr
      WHERE usr.processed_flag = 'N'
      AND usr.run_id = g_run_id
      AND EXISTS(SELECT 1
                        FROM thot.patients_table rxh
                        WHERE rxh.ID = usr.patient_id)
      ORDER BY ROWID;

    TYPE pt_user_flag_types_tab IS TABLE OF pt_user_flag_types_cur%ROWTYPE;

    TYPE pt_user_flag_types_rowid_tab IS TABLE OF ROWID;

    t_pt_user_flag_types        pt_user_flag_types_tab;
    t_pt_user_flag_types_rowid  pt_user_flag_types_rowid_tab;
    t_triggers                  triggers_tab;
    v_context                   rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_PT_USER_FLAG_TYPES';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'PT_USER_FLAG_TYPES', t_triggers => t_triggers);

    OPEN pt_user_flag_types_cur;

    OPEN pt_user_flag_types_rowid_cur;

    LOOP
      FETCH pt_user_flag_types_cur
      BULK COLLECT INTO t_pt_user_flag_types LIMIT bulk_limit;

      EXIT WHEN t_pt_user_flag_types.COUNT = 0;

      FETCH pt_user_flag_types_rowid_cur
      BULK COLLECT INTO t_pt_user_flag_types_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_pt_user_flag_types.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT patient_id
                             ,user_flag_type
                             ,group_code
                             ,start_date
                             ,stop_date
                             ,comments
                             ,partnership_security_label
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,update_date
                         FROM thot.pt_user_flag_types)
               VALUES t_pt_user_flag_types(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.PT_USER_FLAG_TYPES'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_pt_user_flag_types.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT patient_id
                                   ,user_flag_type
                                   ,group_code
                                   ,start_date
                                   ,stop_date
                                   ,comments
                                   ,partnership_security_label
                                   ,created_by
                                   ,creation_date
                                   ,updated_by
                                   ,update_date
                               FROM thot.pt_user_flag_types)
                     VALUES t_pt_user_flag_types(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_pt_user_flag_types(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               ,p_attr2_name    => 'USER_FLAG_TYPE'
                                                               ,p_attr2_value   => t_pt_user_flag_types(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).user_flag_type
                                                               ,p_attr3_name    => 'GROUP_CODE'
                                                               ,p_attr3_value   => t_pt_user_flag_types(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).group_code
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_pt_user_flag_types_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_pt_user_flag_types.COUNT || ' | Records inserted:' ||(t_pt_user_flag_types.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_pt_user_flag_types.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.pt_user_flag_types@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_pt_user_flag_types_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE pt_user_flag_types_cur;

    CLOSE pt_user_flag_types_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF pt_user_flag_types_cur%ISOPEN THEN
        CLOSE pt_user_flag_types_cur;
      END IF;

      IF pt_user_flag_types_rowid_cur%ISOPEN THEN
        CLOSE pt_user_flag_types_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_pt_user_flag_types;

--=======================================+
--| PULL_ICSLOCS: Pulls icslocs table to rxhome.                   |
--=======================================+
  PROCEDURE pull_icslocs
  IS
    CURSOR icslocs_cur
    IS
      SELECT   inventory_id
              ,lot
              ,cmp_id
              ,svcbr_id
              ,loc_type
              ,loc_id
              ,qoh
              ,reserved_qty
              ,available_qty
              ,reorder_qty
              ,last_cost
              ,sell_price
              ,book_amt
              ,status
              ,xdate
              ,xuser
          FROM rxh_stage.icslocs@rxh_stg ics
         WHERE ics.processed_flag = 'N'
           AND ics.run_id = g_run_id
           AND NOT EXISTS(SELECT 1
                            FROM thot.icslocs rxh
                           WHERE rxh.inventory_id = ics.inventory_id
                             AND rxh.lot = ics.lot
                             AND rxh.cmp_id = ics.cmp_id
                             AND rxh.svcbr_id = ics.svcbr_id
                             AND rxh.loc_type = ics.loc_type
                             AND rxh.loc_id = ics.loc_id)
      ORDER BY ROWID;

    CURSOR icslocs_rowid_cur
    IS
      SELECT ROWID row_id
        FROM rxh_stage.icslocs@rxh_stg ics
       WHERE ics.processed_flag = 'N'
         AND ics.run_id = g_run_id
         AND NOT EXISTS(SELECT 1
                          FROM thot.icslocs rxh
                         WHERE rxh.inventory_id = ics.inventory_id
                           AND rxh.lot = ics.lot
                           AND rxh.cmp_id = ics.cmp_id
                           AND rxh.svcbr_id = ics.svcbr_id
                           AND rxh.loc_type = ics.loc_type
                           AND rxh.loc_id = ics.loc_id)
      ORDER BY ROWID;

    TYPE icslocs_tab IS TABLE OF icslocs_cur%ROWTYPE;

    TYPE icslocs_rowid_tab IS TABLE OF ROWID;

    t_icslocs        icslocs_tab;
    t_icslocs_rowid  icslocs_rowid_tab;
    t_triggers       triggers_tab;
    v_context        rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_ICSLOCS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'ICSLOCS', t_triggers => t_triggers);

    OPEN icslocs_cur;

    OPEN icslocs_rowid_cur;

    LOOP
      FETCH icslocs_cur
      BULK COLLECT INTO t_icslocs LIMIT bulk_limit;

      EXIT WHEN t_icslocs.COUNT = 0;

      FETCH icslocs_rowid_cur
      BULK COLLECT INTO t_icslocs_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_icslocs.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT inventory_id
                             ,lot
                             ,cmp_id
                             ,svcbr_id
                             ,loc_type
                             ,loc_id
                             ,qoh
                             ,reserved_qty
                             ,available_qty
                             ,reorder_qty
                             ,last_cost
                             ,sell_price
                             ,book_amt
                             ,status
                             ,xdate
                             ,xuser
                         FROM thot.icslocs)
               VALUES t_icslocs(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.ICSLOCS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_icslocs.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT inventory_id
                                   ,lot
                                   ,cmp_id
                                   ,svcbr_id
                                   ,loc_type
                                   ,loc_id
                                   ,qoh
                                   ,reserved_qty
                                   ,available_qty
                                   ,reorder_qty
                                   ,last_cost
                                   ,sell_price
                                   ,book_amt
                                   ,status
                                   ,xdate
                                   ,xuser
                               FROM thot.icslocs)
                     VALUES t_icslocs(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'INVENTORY_ID|LOT'
                                                               ,p_attr1_value   => t_icslocs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).inventory_id || '|' || t_icslocs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).lot
                                                               ,p_attr2_name    => 'CMP_ID|LOT'
                                                               ,p_attr2_value   => t_icslocs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).cmp_id || '|' || t_icslocs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).lot
                                                               ,p_attr3_name    => 'LOC_TYPE|LOC_ID'
                                                               ,p_attr3_value   => t_icslocs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).loc_type || '|' || t_icslocs(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).loc_id
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_icslocs_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_icslocs.COUNT || ' | Records inserted:' ||(t_icslocs.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_icslocs.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.icslocs@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_icslocs_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE icslocs_cur;

    CLOSE icslocs_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF icslocs_cur%ISOPEN THEN
        CLOSE icslocs_cur;
      END IF;

      IF icslocs_rowid_cur%ISOPEN THEN
        CLOSE icslocs_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_icslocs;

--========================================================+
--| PULL_EXT_SYS_PKEY_XREF: Pulls ext_sys_pkey_xref table to rxhome.                    |
--========================================================+
  PROCEDURE pull_ext_sys_pkey_xref
  IS
    CURSOR ext_sys_pkey_xref_cur
    IS
     SELECT   system_name
                    ,tab_name
                    ,rxh_tab_name
                    ,tab_pkey
                    ,rxh_tab_pkey
      FROM rxh_stage.ext_sys_pkey_xref@rxh_stg ext
      WHERE ext.processed_flag = 'N'
      AND ext.run_id = g_run_id
      AND EXISTS (SELECT 1
                            FROM  thot.ext_sys_tab_xref tab
                            WHERE tab.system_name = ext.system_name
                            AND tab.tab_name = ext.tab_name
                            AND tab.rxh_tab_name = ext.rxh_tab_name)
      ORDER BY ROWID;

    CURSOR ext_sys_pkey_xref_rowid_cur
    IS
      SELECT   ROWID row_id
      FROM rxh_stage.ext_sys_pkey_xref@rxh_stg ext
      WHERE ext.processed_flag = 'N'
      AND ext.run_id = g_run_id
      AND EXISTS (SELECT 1
                          FROM  thot.ext_sys_tab_xref tab
                          WHERE tab.system_name = ext.system_name
                          AND tab.tab_name = ext.tab_name
                          AND tab.rxh_tab_name = ext.rxh_tab_name)
      ORDER BY ROWID;

    TYPE ext_sys_pkey_xref_tab IS TABLE OF ext_sys_pkey_xref_cur%ROWTYPE;

    TYPE ext_sys_pkey_xref_rowid_tab IS TABLE OF ROWID;

    t_ext_sys_pkey_xref           ext_sys_pkey_xref_tab;
    t_ext_sys_pkey_xref_rowid  ext_sys_pkey_xref_rowid_tab;
    t_triggers                           triggers_tab;
    v_context                           rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_EXT_SYS_PKEY_XREF';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'EXT_SYS_PKEY_XREF', t_triggers => t_triggers);

    OPEN ext_sys_pkey_xref_cur;

    OPEN ext_sys_pkey_xref_rowid_cur;

    LOOP
      FETCH ext_sys_pkey_xref_cur
      BULK COLLECT INTO t_ext_sys_pkey_xref LIMIT bulk_limit;

      EXIT WHEN t_ext_sys_pkey_xref.COUNT = 0;

      FETCH ext_sys_pkey_xref_rowid_cur
      BULK COLLECT INTO t_ext_sys_pkey_xref_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_ext_sys_pkey_xref.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT system_name
                             ,tab_name
                             ,rxh_tab_name
                             ,tab_pkey
                             ,rxh_tab_pkey
                         FROM thot.ext_sys_pkey_xref)
               VALUES t_ext_sys_pkey_xref(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.EXT_SYS_PKEY_XREF'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_ext_sys_pkey_xref.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT system_name
                                   ,tab_name
                                   ,rxh_tab_name
                                   ,tab_pkey
                                   ,rxh_tab_pkey
                               FROM thot.ext_sys_pkey_xref)
                     VALUES t_ext_sys_pkey_xref(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'SYSTEM_NAME'
                                                               ,p_attr1_value   => t_ext_sys_pkey_xref(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).system_name
                                                               ,p_attr2_name    => 'TAB_NAME'
                                                               ,p_attr2_value   => t_ext_sys_pkey_xref(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).tab_name
                                                               ,p_attr3_name    => 'RXH_TAB_NAME'
                                                               ,p_attr3_value   => t_ext_sys_pkey_xref(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).rxh_tab_name
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_ext_sys_pkey_xref_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_ext_sys_pkey_xref.COUNT || ' | Records inserted:' ||(t_ext_sys_pkey_xref.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_ext_sys_pkey_xref.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.ext_sys_pkey_xref@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_ext_sys_pkey_xref_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE ext_sys_pkey_xref_cur;

    CLOSE ext_sys_pkey_xref_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF ext_sys_pkey_xref_cur%ISOPEN THEN
        CLOSE ext_sys_pkey_xref_cur;
      END IF;

      IF ext_sys_pkey_xref_rowid_cur%ISOPEN THEN
        CLOSE ext_sys_pkey_xref_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_ext_sys_pkey_xref;

--========================================================+
--| PULL_WQ_TASKS: Pulls wq_tasks table to rxhome.                                                 |
--========================================================+
  PROCEDURE pull_wq_tasks
  IS
    CURSOR wq_tasks_cur
    IS
      SELECT  task_id
                  ,description
                  ,source_type
                  ,source_id
                  ,svcbr_id
                  ,patient_id
                  ,prescription_id
                  ,refill_no
                  ,workqueue_id
                  ,team_assignment_id
                  ,xuser
                  ,comments
                  ,last_status_date
                  ,locked_by
                  ,locked_date
                  ,task_date
                  ,task_status_id
                  ,created_by
                  ,creation_date
                  ,updated_by
                  ,update_date
                  ,assignment_rule_id
                  ,source_id1
                  ,source_id2
                  ,source_id3
                  ,task_sts_reason_code_id
                  ,task_type_id
                  ,task_priority
                  ,chart_notes
                  ,fep_client_flag
                  ,therapy_type
                  ,session_id
                  ,session_assessment_id
                  ,contact_type
                  ,completed_required
                  ,work_order_id
                  ,attribute01
                  ,attribute02
                  ,attribute03
                  ,attribute04
                  ,attribute05
                  ,attribute06
                  ,attribute07
                  ,attribute08
                  ,attribute09
                  ,attribute10
                  ,invoice_id
                  ,invoice_seq
          FROM rxh_stage.wq_tasks@rxh_stg wq
         WHERE wq.processed_flag = 'N'
          AND wq.run_id = g_run_id
          AND NOT EXISTS (SELECT 1
                                       FROM thot.wq_tasks rxh
                                       WHERE rxh.task_id = wq.task_id)
          AND EXISTS(SELECT 1
                                  FROM thot.patients_table pt
                                 WHERE pt.ID = wq.patient_id)
          ORDER BY ROWID;

    CURSOR wq_tasks_rowid_cur
    IS
      SELECT   ROWID row_id
          FROM rxh_stage.wq_tasks@rxh_stg wq
          WHERE wq.processed_flag = 'N'
          AND wq.run_id = g_run_id
          AND NOT EXISTS (SELECT 1
                                       FROM thot.wq_tasks rxh
                                       WHERE rxh.task_id = wq.task_id)
          AND EXISTS(SELECT 1
                                  FROM thot.patients_table pt
                                 WHERE pt.ID = wq.patient_id)
      ORDER BY ROWID;

    TYPE wq_tasks_tab IS TABLE OF wq_tasks_cur%ROWTYPE;

    TYPE wq_tasks_rowid_tab IS TABLE OF ROWID;

    t_wq_tasks          wq_tasks_tab;
    t_wq_tasks_rowid  wq_tasks_rowid_tab;
    t_triggers                  triggers_tab;
    v_context                  rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_WQ_TASKS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'WQ_TASKS', t_triggers => t_triggers);

    OPEN wq_tasks_cur;

    OPEN wq_tasks_rowid_cur;

    LOOP
      FETCH wq_tasks_cur
      BULK COLLECT INTO t_wq_tasks LIMIT bulk_limit;

      EXIT WHEN t_wq_tasks.COUNT = 0;

      FETCH wq_tasks_rowid_cur
      BULK COLLECT INTO t_wq_tasks_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_wq_tasks.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT task_id
                                              ,description
                                              ,source_type
                                              ,source_id
                                              ,svcbr_id
                                              ,patient_id
                                              ,prescription_id
                                              ,refill_no
                                              ,workqueue_id
                                              ,team_assignment_id
                                              ,xuser
                                              ,comments
                                              ,last_status_date
                                              ,locked_by
                                              ,locked_date
                                              ,task_date
                                              ,task_status_id
                                              ,created_by
                                              ,creation_date
                                              ,updated_by
                                              ,update_date
                                              ,assignment_rule_id
                                              ,source_id1
                                              ,source_id2
                                              ,source_id3
                                              ,task_sts_reason_code_id
                                              ,task_type_id
                                              ,task_priority
                                              ,chart_notes
                                              ,fep_client_flag
                                              ,therapy_type
                                              ,session_id
                                              ,session_assessment_id
                                              ,contact_type
                                              ,completed_required
                                              ,work_order_id
                                              ,attribute01
                                              ,attribute02
                                              ,attribute03
                                              ,attribute04
                                              ,attribute05
                                              ,attribute06
                                              ,attribute07
                                              ,attribute08
                                              ,attribute09
                                              ,attribute10
                                              ,invoice_id
                                              ,invoice_seq
                         FROM thot.wq_tasks)
               VALUES t_wq_tasks(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.WQ_TASKS'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_wq_tasks.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT task_id
                                    ,description
                                    ,source_type
                                    ,source_id
                                    ,svcbr_id
                                    ,patient_id
                                    ,prescription_id
                                    ,refill_no
                                    ,workqueue_id
                                    ,team_assignment_id
                                    ,xuser
                                    ,comments
                                    ,last_status_date
                                    ,locked_by
                                    ,locked_date
                                    ,task_date
                                    ,task_status_id
                                    ,created_by
                                    ,creation_date
                                    ,updated_by
                                    ,update_date
                                    ,assignment_rule_id
                                    ,source_id1
                                    ,source_id2
                                    ,source_id3
                                    ,task_sts_reason_code_id
                                    ,task_type_id
                                    ,task_priority
                                    ,chart_notes
                                    ,fep_client_flag
                                    ,therapy_type
                                    ,session_id
                                    ,session_assessment_id
                                    ,contact_type
                                    ,completed_required
                                    ,work_order_id
                                    ,attribute01
                                    ,attribute02
                                    ,attribute03
                                    ,attribute04
                                    ,attribute05
                                    ,attribute06
                                    ,attribute07
                                    ,attribute08
                                    ,attribute09
                                    ,attribute10
                                    ,invoice_id
                                    ,invoice_seq
                               FROM thot.wq_tasks)
                     VALUES t_wq_tasks(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name   => 'SVCBR_ID|PRESCRIPTION_ID|REFILL_NO'
                                                               ,p_attr1_value   =>    t_wq_tasks(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).svcbr_id
                                                                                               || '|'
                                                                                               || t_wq_tasks(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).prescription_id
                                                                                               || '|'
                                                                                               || t_wq_tasks(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).refill_no
                                                               ,p_attr2_name    => 'PATIENT_ID'
                                                               ,p_attr2_value   => t_wq_tasks(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).PATIENT_ID
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_wq_tasks_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_wq_tasks.COUNT || ' | Records inserted:' ||(t_wq_tasks.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_wq_tasks.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.wq_tasks@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_wq_tasks_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE wq_tasks_cur;

    CLOSE wq_tasks_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF wq_tasks_cur%ISOPEN THEN
        CLOSE wq_tasks_cur;
      END IF;

      IF wq_tasks_rowid_cur%ISOPEN THEN
        CLOSE wq_tasks_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_wq_tasks;

--========================================================+
--| PULL_WQ_TASKS_HISTORY: Pulls wq_tasks_history table to rxhome.                      |
--========================================================+
  PROCEDURE pull_wq_tasks_history
  IS
    CURSOR wq_tasks_history_cur
    IS
      SELECT   history_id
                    ,task_id
                    ,description
                    ,source_type
                    ,source_id
                    ,patient_id
                    ,svcbr_id
                    ,prescription_id
                    ,refill_no
                    ,workqueue_id
                    ,team_assignment_id
                    ,xuser
                    ,assignment_rule_id
                    ,comments
                    ,last_status_date
                    ,locked_by
                    ,locked_date
                    ,task_date
                    ,task_status_id
                    ,created_by
                    ,creation_date
                    ,updated_by
                    ,update_date
                    ,source_id1
                    ,source_id2
                    ,source_id3
                    ,task_sts_reason_code_id
                    ,task_type_id
                    ,task_priority
                    ,chart_notes
                    ,contact_type
          FROM rxh_stage.wq_tasks_history@rxh_stg wq
         WHERE wq.processed_flag = 'N'
          AND wq.run_id = g_run_id
          AND NOT EXISTS (SELECT 1
                                       FROM thot.wq_tasks_history rxh
                                       WHERE rxh.history_id = wq.history_id)
          AND EXISTS (SELECT 1
                                 FROM thot.wq_tasks wrx
                                 WHERE wrx.task_id = wq.task_id)
          ORDER BY ROWID;

    CURSOR wq_tasks_history_rowid_cur
    IS
      SELECT   ROWID row_id
      FROM rxh_stage.wq_tasks_history@rxh_stg wq
      WHERE wq.processed_flag = 'N'
      AND wq.run_id = g_run_id
      AND NOT EXISTS (SELECT 1
                                   FROM thot.wq_tasks_history rxh
                                   WHERE rxh.history_id = wq.history_id)
      AND EXISTS (SELECT 1
                             FROM thot.wq_tasks wrx
                             WHERE wrx.task_id = wq.task_id)
      ORDER BY ROWID;

    TYPE wq_tasks_history_tab IS TABLE OF wq_tasks_history_cur%ROWTYPE;

    TYPE wq_tasks_history_rowid_tab IS TABLE OF ROWID;

    t_wq_tasks_history           wq_tasks_history_tab;
    t_wq_tasks_history_rowid  wq_tasks_history_rowid_tab;
    t_triggers                         triggers_tab;
    v_context                         rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.PULL_WQ_TASKS_HISTORY';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'THOT', p_table_name => 'WQ_TASKS_HISTORY', t_triggers => t_triggers);

    OPEN wq_tasks_history_cur;

    OPEN wq_tasks_history_rowid_cur;

    LOOP
      FETCH wq_tasks_history_cur
      BULK COLLECT INTO t_wq_tasks_history LIMIT bulk_limit;

      EXIT WHEN t_wq_tasks_history.COUNT = 0;

      FETCH wq_tasks_history_rowid_cur
      BULK COLLECT INTO t_wq_tasks_history_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_wq_tasks_history.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT history_id
                                              ,task_id
                                              ,description
                                              ,source_type
                                              ,source_id
                                              ,patient_id
                                              ,svcbr_id
                                              ,prescription_id
                                              ,refill_no
                                              ,workqueue_id
                                              ,team_assignment_id
                                              ,xuser
                                              ,assignment_rule_id
                                              ,comments
                                              ,last_status_date
                                              ,locked_by
                                              ,locked_date
                                              ,task_date
                                              ,task_status_id
                                              ,created_by
                                              ,creation_date
                                              ,updated_by
                                              ,update_date
                                              ,source_id1
                                              ,source_id2
                                              ,source_id3
                                              ,task_sts_reason_code_id
                                              ,task_type_id
                                              ,task_priority
                                              ,chart_notes
                                              ,contact_type
                         FROM thot.wq_tasks_history)
               VALUES t_wq_tasks_history(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.WQ_TASKS_HISTORY'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_wq_tasks_history.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT history_id
                                    ,task_id
                                    ,description
                                    ,source_type
                                    ,source_id
                                    ,patient_id
                                    ,svcbr_id
                                    ,prescription_id
                                    ,refill_no
                                    ,workqueue_id
                                    ,team_assignment_id
                                    ,xuser
                                    ,assignment_rule_id
                                    ,comments
                                    ,last_status_date
                                    ,locked_by
                                    ,locked_date
                                    ,task_date
                                    ,task_status_id
                                    ,created_by
                                    ,creation_date
                                    ,updated_by
                                    ,update_date
                                    ,source_id1
                                    ,source_id2
                                    ,source_id3
                                    ,task_sts_reason_code_id
                                    ,task_type_id
                                    ,task_priority
                                    ,chart_notes
                                    ,contact_type
                               FROM thot.wq_tasks_history)
                     VALUES t_wq_tasks_history(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name   => 'SVCBR_ID|PRESCRIPTION_ID|REFILL_NO'
                                                               ,p_attr1_value   => t_wq_tasks_history(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).svcbr_id
                                                                                   || '|'
                                                                                   || t_wq_tasks_history(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).prescription_id
                                                                                   || '|'
                                                                                   || t_wq_tasks_history(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).refill_no
                                                               ,p_attr2_name    => 'PATIENT_ID'
                                                               ,p_attr2_value   => t_wq_tasks_history(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).PATIENT_ID
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_wq_tasks_history_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context => v_context,
                                              p_error_msg => 'Records processed :' || t_wq_tasks_history.COUNT || ' | Records inserted:' ||(t_wq_tasks_history.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_wq_tasks_history.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.wq_tasks_history@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_wq_tasks_history_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE wq_tasks_history_cur;

    CLOSE wq_tasks_history_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF wq_tasks_history_cur%ISOPEN THEN
        CLOSE wq_tasks_history_cur;
      END IF;

      IF wq_tasks_history_rowid_cur%ISOPEN THEN
        CLOSE wq_tasks_history_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context   => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context  => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_wq_tasks_history;


--========================================================+
--| PULL_MS_SDDB: Pulls ms_sddb table to rxhome.          |
--========================================================+
  PROCEDURE pull_ms_sddb
  IS
    CURSOR ms_sddb_cur
    IS
      SELECT rec_cd
             ,act_cd
             ,id_form_cd
             ,ndc_upc_hri
             ,dea_class
             ,rx_otc_ind
             ,third_party
             ,prod_descr_abbr
             ,total_package_qty
             ,size_u_m
             ,pack_qty
             ,drug_name_cd
             ,thera_class
             ,gen_typ_cd
             ,gen_id_no
             ,ud_uu_pkg
             ,desi
             ,awp_date
             ,awp_unit_price
             ,dp_unit_price
             ,reserved
             ,sup_by_fmt_cd
             ,sup_by_id_no
             ,gen_cd
             ,awp_source
             ,prod_cat_code
             ,form_code
             ,solid_liquid_ind
             ,strength_descr
             ,manufacturer_name
             ,gcr_code
             ,gfc_code
             ,ful_effective_date
             ,ful_unit_price
             ,roa_code
             ,master_form_code
             ,ndc_inactive_date
             ,xdate
             ,prev_ndc_id_no
             ,prev_ndc_id_cd
      FROM rxh_stage.ms_sddb@rxh_stg wq
      WHERE wq.processed_flag = 'N'
      AND wq.run_id = g_run_id
      AND NOT EXISTS (SELECT 1
                      FROM thot.ms_sddb rxh
                      WHERE rxh.awp_source = wq.awp_source
                      AND rxh.ndc_upc_hri = wq.ndc_upc_hri)
      ORDER BY ROWID;

    CURSOR ms_sddb_rowid_cur
    IS
      SELECT ROWID row_id
      FROM rxh_stage.ms_sddb@rxh_stg wq
      WHERE wq.processed_flag = 'N'
      AND wq.run_id = g_run_id
      AND NOT EXISTS (SELECT 1
                      FROM thot.ms_sddb rxh
                      WHERE  rxh.awp_source = wq.awp_source
                      AND rxh.ndc_upc_hri = wq.ndc_upc_hri)
      ORDER BY ROWID;

    TYPE ms_sddb_tab IS TABLE OF ms_sddb_cur%ROWTYPE;

    TYPE ms_sddb_rowid_tab IS TABLE OF ROWID;

    t_ms_sddb           ms_sddb_tab;
    t_ms_sddb_rowid     ms_sddb_rowid_tab;
    t_triggers          triggers_tab;
    v_context           rxh_stage.a2r_message_log.context@rxh_stg%TYPE := 'A2R_MIGRATION_PULL_PKG.PULL_MS_SDDB';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner => 'THOT', p_table_name => 'MS_SDDB', t_triggers => t_triggers);

    OPEN ms_sddb_cur;

    OPEN ms_sddb_rowid_cur;

    LOOP
      FETCH ms_sddb_cur
      BULK COLLECT INTO t_ms_sddb LIMIT bulk_limit;

      EXIT WHEN t_ms_sddb.COUNT = 0;

      FETCH ms_sddb_rowid_cur
      BULK COLLECT INTO t_ms_sddb_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_ms_sddb.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT rec_cd
                              ,act_cd
                              ,id_form_cd
                              ,ndc_upc_hri
                              ,dea_class
                              ,rx_otc_ind
                              ,third_party
                              ,prod_descr_abbr
                              ,total_package_qty
                              ,size_u_m
                              ,pack_qty
                              ,drug_name_cd
                              ,thera_class
                              ,gen_typ_cd
                              ,gen_id_no
                              ,ud_uu_pkg
                              ,desi
                              ,awp_date
                              ,awp_unit_price
                              ,dp_unit_price
                              ,reserved
                              ,sup_by_fmt_cd
                              ,sup_by_id_no
                              ,gen_cd
                              ,awp_source
                              ,prod_cat_code
                              ,form_code
                              ,solid_liquid_ind
                              ,strength_descr
                              ,manufacturer_name
                              ,gcr_code
                              ,gfc_code
                              ,ful_effective_date
                              ,ful_unit_price
                              ,roa_code
                              ,master_form_code
                              ,ndc_inactive_date
                              ,xdate
                              ,prev_ndc_id_no
                              ,prev_ndc_id_cd
                       FROM thot.ms_sddb)
               VALUES t_ms_sddb(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN

            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.MS_SDDB'
                                                             , p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_ms_sddb.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
             g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                INSERT INTO (SELECT rec_cd
                              ,act_cd
                              ,id_form_cd
                              ,ndc_upc_hri
                              ,dea_class
                              ,rx_otc_ind
                              ,third_party
                              ,prod_descr_abbr
                              ,total_package_qty
                              ,size_u_m
                              ,pack_qty
                              ,drug_name_cd
                              ,thera_class
                              ,gen_typ_cd
                              ,gen_id_no
                              ,ud_uu_pkg
                              ,desi
                              ,awp_date
                              ,awp_unit_price
                              ,dp_unit_price
                              ,reserved
                              ,sup_by_fmt_cd
                              ,sup_by_id_no
                              ,gen_cd
                              ,awp_source
                              ,prod_cat_code
                              ,form_code
                              ,solid_liquid_ind
                              ,strength_descr
                              ,manufacturer_name
                              ,gcr_code
                              ,gfc_code
                              ,ful_effective_date
                              ,ful_unit_price
                              ,roa_code
                              ,master_form_code
                              ,ndc_inactive_date
                              ,xdate
                              ,prev_ndc_id_no
                              ,prev_ndc_id_cd
                       FROM thot.ms_sddb)
               VALUES t_ms_sddb(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;
              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'AWP_SOURCE'
                                                               ,p_attr1_value   => t_ms_sddb(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).AWP_SOURCE
                                                               ,p_attr2_name    => 'NDC_UPC_HRI'
                                                               ,p_attr2_value   => t_ms_sddb(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).NDC_UPC_HRI
                                                               , p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_ms_sddb_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context => v_context,
                                              p_error_msg => 'Records processed :' || t_ms_sddb.COUNT || ' | Records inserted:' ||(t_ms_sddb.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              , p_run_id => g_run_id);

      FOR i IN 1 .. t_ms_sddb.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.ms_sddb@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_ms_sddb_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE ms_sddb_cur;

    CLOSE ms_sddb_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF ms_sddb_cur%ISOPEN THEN
        CLOSE ms_sddb_cur;
      END IF;

      IF ms_sddb_rowid_cur%ISOPEN THEN
        CLOSE ms_sddb_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context    => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_ms_sddb;
--|========================================================+
--| PULL_CC_PT_DETAILS: Pulls CREDIT_CARD_PT_DETAILS table to rxhome.          |
--|========================================================+
  PROCEDURE pull_cc_pt_details
  IS
    CURSOR cc_pt_details_cur
    IS
      SELECT card_pt_detail_id
            ,card_key_id
            ,patient_id
            ,therapy_type
            ,created_by
            ,creation_date
            ,updated_by
            ,updated_date
      FROM rxh_stage.credit_card_pt_details@rxh_stg wq
      WHERE wq.processed_flag = 'N'
      AND wq.run_id = g_run_id
      AND NOT EXISTS (SELECT 1
                      FROM thot.credit_card_pt_details rxh
                      WHERE rxh.card_pt_detail_id = wq.card_pt_detail_id)
      AND EXISTS (SELECT 1
                  FROM thot.patients_table p
                  where p.id = wq.patient_id)
      ORDER BY ROWID;

    CURSOR cc_pt_details_rowid_cur
    IS
      SELECT ROWID row_id
      FROM rxh_stage.credit_card_pt_details@rxh_stg wq
      WHERE wq.processed_flag = 'N'
      AND wq.run_id = g_run_id
      AND NOT EXISTS (SELECT 1
                      FROM thot.credit_card_pt_details rxh
                      WHERE rxh.card_pt_detail_id = wq.card_pt_detail_id)
      AND EXISTS (SELECT 1
                  FROM thot.patients_table p
                  where p.id = wq.patient_id)
      ORDER BY ROWID;

    TYPE cc_pt_details_tab IS TABLE OF cc_pt_details_cur%ROWTYPE;

    TYPE cc_pt_details_rowid_tab IS TABLE OF ROWID;

    t_cc_pt_details           cc_pt_details_tab;
    t_cc_pt_details_rowid     cc_pt_details_rowid_tab;
    t_triggers          triggers_tab;
    v_context           rxh_stage.a2r_message_log.context@rxh_stg%TYPE := 'A2R_MIGRATION_PULL_PKG.PULL_CC_PT_DETAILS';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner => 'THOT', p_table_name => 'CREDIT_CARD_PT_DETAILS', t_triggers => t_triggers);

    OPEN cc_pt_details_cur;

    OPEN cc_pt_details_rowid_cur;

    LOOP
      FETCH cc_pt_details_cur
      BULK COLLECT INTO t_cc_pt_details LIMIT bulk_limit;

      EXIT WHEN t_cc_pt_details.COUNT = 0;

      FETCH cc_pt_details_rowid_cur
      BULK COLLECT INTO t_cc_pt_details_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_cc_pt_details.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT card_pt_detail_id
                            ,card_key_id
                            ,patient_id
                            ,therapy_type
                            ,created_by
                            ,creation_date
                            ,updated_by
                            ,updated_date
                       FROM thot.credit_card_pt_details)
               VALUES t_cc_pt_details(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.CREDIT_CARD_PT_DETAILS'
                                                             ,p_run_id     => g_run_id);
            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_cc_pt_details.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                INSERT INTO (SELECT card_pt_detail_id
                            ,card_key_id
                            ,patient_id
                            ,therapy_type
                            ,created_by
                            ,creation_date
                            ,updated_by
                            ,updated_date
                       FROM thot.credit_card_pt_details)
               VALUES t_cc_pt_details(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;
              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg  --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'CARD_KEY_ID'
                                                               ,p_attr1_value   => t_cc_pt_details(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).card_key_id
                                                               ,p_attr2_name    => 'PATIENT_ID'
                                                               ,p_attr2_value   => t_cc_pt_details(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).patient_id
                                                               ,p_run_id      => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_cc_pt_details_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context => v_context,
                                              p_error_msg => 'Records processed :' || t_cc_pt_details.COUNT || ' | Records inserted:' ||(t_cc_pt_details.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              ,p_run_id => g_run_id);

      FOR i IN 1 .. t_cc_pt_details.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.credit_card_pt_details@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_cc_pt_details_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE cc_pt_details_cur;

    CLOSE cc_pt_details_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF cc_pt_details_cur%ISOPEN THEN
        CLOSE cc_pt_details_cur;
      END IF;

      IF cc_pt_details_rowid_cur%ISOPEN THEN
        CLOSE cc_pt_details_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context    => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_cc_pt_details;

--|========================================================+
--| PULL_SUBSCRIBER_EMPLOYER: Pulls SUBSCRIBER_EMPLOYER table to rxhome.          |
--|========================================================+
  PROCEDURE pull_subscriber_employer
  IS
    CURSOR employer_cur
    IS
      SELECT employer_id
            ,group_no
            ,employer_name
            ,address1
            ,address2
            ,city
            ,state
            ,zip1
            ,zip2
            ,country
            ,phone_type1
            ,phone1a
            ,phone1b
            ,phone1c
            ,phone_ext1
            ,phone_type2
            ,phone2a
            ,phone2b
            ,phone2c
            ,phone_ext2
            ,created_by
            ,creation_date
      FROM rxh_stage.subscriber_employer@rxh_stg wq
      WHERE wq.processed_flag = 'N'
--      AND wq.run_id = g_run_id
      AND NOT EXISTS (SELECT 1
                      FROM thot.subscriber_employer rxh
                      WHERE rxh.employer_id = wq.employer_id)
      AND NOT EXISTS (SELECT 1
                      FROM thot.subscriber_employer se
                      WHERE (se.group_no = wq.group_no or(se.group_no is null and wq.group_no is null))
                      and se.employer_name = wq.employer_name)
      ORDER BY ROWID;

    CURSOR employer_rowid_cur
    IS
      SELECT ROWID row_id
      FROM rxh_stage.subscriber_employer@rxh_stg wq
      WHERE wq.processed_flag = 'N'
--      AND wq.run_id = g_run_id
      AND NOT EXISTS (SELECT 1
                      FROM thot.subscriber_employer rxh
                      WHERE rxh.employer_id = wq.employer_id)
      AND NOT EXISTS (SELECT 1
                      FROM thot.subscriber_employer se
                      WHERE (se.group_no = wq.group_no OR(se.group_no IS NULL AND wq.group_no IS NULL))
                      and se.employer_name = wq.employer_name)
      ORDER BY ROWID;

    TYPE employer_tab IS TABLE OF employer_cur%ROWTYPE;

    TYPE employer_rowid_tab IS TABLE OF ROWID;

    t_employer           employer_tab;
    t_employer_rowid     employer_rowid_tab;
    t_triggers          triggers_tab;
    v_context           rxh_stage.a2r_message_log.context@rxh_stg%TYPE := 'A2R_MIGRATION_PULL_PKG.SUBSCRIBER_EMPLOYER';
  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner => 'THOT', p_table_name => 'CREDIT_CARD_PT_DETAILS', t_triggers => t_triggers);

    OPEN employer_cur;

    OPEN employer_rowid_cur;

    LOOP
      FETCH employer_cur
      BULK COLLECT INTO t_employer LIMIT bulk_limit;

      EXIT WHEN t_employer.COUNT = 0;

      FETCH employer_rowid_cur
      BULK COLLECT INTO t_employer_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_employer.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT employer_id
                              ,group_no
                              ,employer_name
                              ,address1
                              ,address2
                              ,city
                              ,state
                              ,zip1
                              ,zip2
                              ,country
                              ,phone_type1
                              ,phone1a
                              ,phone1b
                              ,phone1c
                              ,phone_ext1
                              ,phone_type2
                              ,phone2a
                              ,phone2b
                              ,phone2c
                              ,phone_ext2
                              ,created_by
                              ,creation_date
                       FROM thot.subscriber_employer)
               VALUES t_employer(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'THOT.SUBSCRIBER_EMPLOYER'
                                                             ,p_run_id => g_run_id);

            -- Get current Persentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_employer.COUNT,SQL%BULK_EXCEPTIONS.COUNT);

            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;

              BEGIN
              --
                INSERT INTO (SELECT employer_id
                                    ,group_no
                                    ,employer_name
                                    ,address1
                                    ,address2
                                    ,city
                                    ,state
                                    ,zip1
                                    ,zip2
                                    ,country
                                    ,phone_type1
                                    ,phone1a
                                    ,phone1b
                                    ,phone1c
                                    ,phone_ext1
                                    ,phone_type2
                                    ,phone2a
                                    ,phone2b
                                    ,phone2c
                                    ,phone_ext2
                                    ,created_by
                                    ,creation_date
                             FROM thot.subscriber_employer)
                     VALUES t_employer(g_index_err);

              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg
                                                               ,p_attr1_name    => 'EMPLOYER_ID'
                                                               ,p_attr1_value   => t_employer(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).employer_id
                                                               ,p_attr2_name    => 'GROUP_NO'
                                                               ,p_attr2_value   => t_employer(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).group_no
                                                               ,p_run_id => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_employer_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
              g_index_err := NULL;
              g_error_msg := NULL;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context => v_context,
                                              p_error_msg => 'Records processed :' || t_employer.COUNT || ' | Records inserted:' ||(t_employer.COUNT - SQL%BULK_EXCEPTIONS.COUNT)
                                              ,p_run_id => g_run_id);

      FOR i IN 1 .. t_employer.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.subscriber_employer@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_employer_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE employer_cur;

    CLOSE employer_rowid_cur;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF employer_cur%ISOPEN THEN
        CLOSE employer_cur;
      END IF;

      IF employer_rowid_cur%ISOPEN THEN
        CLOSE employer_rowid_cur;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context    => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_subscriber_employer;
--=============================================+
--| PULL_MI_PATIENTS_XREF: Pulls PULL_MI_PATIENTS_XREF table to rxhome.  --CQ22731 New procedure for TIER 3
--=============================================+
  PROCEDURE pull_mi_patients_xref
  IS
    CURSOR patients_xref_c
    IS
      SELECT ns_ord_member_id
            ,ns_ord_group
            ,ns_ord_subgroup
            ,ns_rx_dep_no
            ,ID
            ,ns_rx_pat_name_fst
            ,ns_rx_pat_dob
            ,ns_cus_custno
            ,ns_cus_group
            ,ns_cus_subgroup
            ,ns_cus_ind_agn_id
            ,ns_member_first_name
            ,ns_member_last_name
            ,ns_ord_benefit_group
            ,created_by
            ,creation_date
            ,updated_by
            ,update_date
        FROM rxh_stage.mi_patients_xref@rxh_stg ptx
       WHERE ptx.processed_flag = 'N' --CQ22863
         AND ptx.ready_flag = 'Y'
         AND ptx.run_id = g_run_id
         AND EXISTS (SELECT 1
                       FROM thot.patients_table rxh
                      WHERE rxh.ID = ptx.ID)
         AND NOT EXISTS (SELECT 1
                           FROM rxh_custom.mi_patients_xref rxh
                          WHERE rxh.ID = ptx.ID);
      -->> CQ22816 Cursor was changed by new one

      -->> --CQ22967 Adding rowid cursor
    CURSOR patients_xref_rowid_c
    IS
      SELECT ROWID row_id
        FROM rxh_stage.mi_patients_xref@rxh_stg ptx
       WHERE ptx.processed_flag = 'N' --CQ22863
         AND ptx.ready_flag = 'Y'
         AND ptx.run_id = g_run_id
         AND EXISTS (SELECT 1
                       FROM thot.patients_table rxh
                      WHERE rxh.ID = ptx.ID)
         AND NOT EXISTS (SELECT 1
                           FROM rxh_custom.mi_patients_xref rxh
                          WHERE rxh.ID = ptx.ID);

    TYPE pt_xref_tab IS TABLE OF patients_xref_c%ROWTYPE;

    TYPE pt_xref_rowid IS TABLE OF ROWID; --CQ22967

    t_pt_xref          pt_xref_tab;
    t_pt_xref_rowid    pt_xref_rowid;     --CQ22967
    t_triggers         triggers_tab;
    v_context          rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.MI_PATIENTS_XREF';

  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'RXH_CUSTOM', p_table_name => 'MI_PATIENTS_XREF', t_triggers => t_triggers);

    OPEN patients_xref_c;

    OPEN patients_xref_rowid_c;           --CQ22967

    LOOP
      FETCH patients_xref_c
      BULK COLLECT INTO t_pt_xref LIMIT bulk_limit;

      EXIT WHEN t_pt_xref.COUNT = 0;

      FETCH patients_xref_rowid_c
      BULK COLLECT INTO t_pt_xref_rowid LIMIT bulk_limit;     --CQ22967

      BEGIN
        FORALL i IN 1 .. t_pt_xref.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT ns_ord_member_id
                             ,ns_ord_group
                             ,ns_ord_subgroup
                             ,ns_rx_dep_no
                             ,ID
                             ,ns_rx_pat_name_fst
                             ,ns_rx_pat_dob
                             ,ns_cus_custno
                             ,ns_cus_group
                             ,ns_cus_subgroup
                             ,ns_cus_ind_agn_id
                             ,ns_member_first_name
                             ,ns_member_last_name
                             ,ns_ord_benefit_group
                             ,created_by
                             ,creation_date
                             ,updated_by
                             ,update_date
                         FROM rxh_custom.mi_patients_xref)
               VALUES t_pt_xref(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'RXH_CUSTOM.MI_PATIENTS_XREF'
                                                             ,p_run_id     => g_run_id);
            -- Get current Percentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_pt_xref.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                INSERT INTO (SELECT ns_ord_member_id
                                   ,ns_ord_group
                                   ,ns_ord_subgroup
                                   ,ns_rx_dep_no
                                   ,ID
                                   ,ns_rx_pat_name_fst
                                   ,ns_rx_pat_dob
                                   ,ns_cus_custno
                                   ,ns_cus_group
                                   ,ns_cus_subgroup
                                   ,ns_cus_ind_agn_id
                                   ,ns_member_first_name
                                   ,ns_member_last_name
                                   ,ns_ord_benefit_group
                                   ,created_by
                                   ,creation_date
                                   ,updated_by
                                   ,update_date
                               FROM rxh_custom.mi_patients_xref)
                            VALUES t_pt_xref(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_pt_xref(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).id
                                                               ,p_run_id     => g_run_id);

              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_pt_xref_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
               -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_pt_xref.COUNT || ' | Records inserted:' ||(t_pt_xref.COUNT - SQL%BULK_EXCEPTIONS.COUNT), p_run_id => g_run_id);


      FOR i IN 1 .. t_pt_xref.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.mi_patients_xref@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_pt_xref_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;   --CQ22967

      COMMIT;
    END LOOP;

    CLOSE patients_xref_c;

    CLOSE patients_xref_rowid_c;    --CQ22967

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF patients_xref_c%ISOPEN THEN
        CLOSE patients_xref_c;
      END IF;

      IF patients_xref_rowid_c%ISOPEN THEN
        CLOSE patients_xref_rowid_c;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_mi_patients_xref;

--=============================================+
--| MI_EXT_PRESCRIPTION_XREF: Pulls MI_EXT_PRESCRIPTION_XREF table to rxhome.  --CQ22731 New procedure for TIER 3
--| CQ22967 - Adding audit columns
--=============================================+
  PROCEDURE pull_mi_ext_prescription_xref
  IS
    CURSOR prescription_xref_c
    IS
      SELECT  atlas_order_no
             ,atlas_patient_id
             ,atlas_rx_with_location
             ,drug_route_ind
             ,processed_by_channel_ind
             ,rxhome_patient_id
             ,creation_date
             ,created_by
             ,update_date
             ,updated_by
        FROM rxh_stage.mi_ext_prescription_xref@rxh_stg px
       WHERE px.ready_flag = 'Y'
         AND px.run_id = g_run_id
         AND EXISTS(SELECT 1
                      FROM thot.patients_table rxh
                     WHERE rxh.ID = px.rxhome_patient_id)
         AND NOT EXISTS(SELECT 1
                          FROM rxh_custom.mi_ext_prescription_xref rxh
                         WHERE rxh.rxhome_patient_id = px.rxhome_patient_id);

    CURSOR prescription_xref_rowid_c
    IS
      SELECT ROWID row_id
        FROM rxh_stage.mi_ext_prescription_xref@rxh_stg px
       WHERE px.ready_flag = 'Y'
         AND px.run_id = g_run_id
         AND EXISTS(SELECT 1
                      FROM thot.patients_table rxh
                     WHERE rxh.ID = px.rxhome_patient_id)
         AND NOT EXISTS(SELECT 1
                          FROM rxh_custom.mi_ext_prescription_xref rxh
                         WHERE rxh.rxhome_patient_id = px.rxhome_patient_id);

    TYPE rx_xref_tab IS TABLE OF prescription_xref_c%ROWTYPE;

    TYPE rx_xref_rowid IS TABLE OF ROWID;

    t_rx_xref          rx_xref_tab;
    t_rx_xref_rowid    rx_xref_rowid;
    t_triggers         triggers_tab;
    v_context          rxh_stage.a2r_message_log.CONTEXT@rxh_stg%TYPE     := 'A2R_MIGRATION_PULL_PKG.MI_EXT_PRESCRIPTION_XREF';

  BEGIN
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context => v_context, p_error_msg => 'Procedure started', p_run_id => g_run_id);
    -- CQ24083 jarcega Commented disable_triggers(p_table_owner   => 'RXH_CUSTOM', p_table_name => 'MI_EXT_PRESCRIPTION_XREF', t_triggers => t_triggers);

    OPEN prescription_xref_c;

    OPEN prescription_xref_rowid_c;

    LOOP
      FETCH prescription_xref_c
      BULK COLLECT INTO t_rx_xref LIMIT bulk_limit;

      EXIT WHEN t_rx_xref.COUNT = 0;

      FETCH prescription_xref_rowid_c
      BULK COLLECT INTO t_rx_xref_rowid LIMIT bulk_limit;

      BEGIN
        FORALL i IN 1 .. t_rx_xref.COUNT SAVE EXCEPTIONS
          INSERT INTO (SELECT  atlas_order_no
                              ,atlas_patient_id
                              ,atlas_rx_with_location
                              ,drug_route_ind
                              ,processed_by_channel_ind
                              ,rxhome_patient_id
                              ,creation_date
                              ,created_by
                              ,update_date
                              ,updated_by
                         FROM rxh_custom.mi_ext_prescription_xref)
               VALUES t_rx_xref(i);
      EXCEPTION
        WHEN bulk_errors THEN
          DECLARE
            v_id  rxh_stage.a2r_message_log.ID@rxh_stg%TYPE;
          BEGIN
            rxh_stage.a2r_log_pkg.log_bulk_errors_hdr@rxh_stg(x_id            => v_id
                                                             ,p_context       => v_context
                                                             ,p_error_code    => SQLCODE
                                                             ,p_error_msg     => SQLERRM
                                                             ,p_attr1_name    => 'TABLE_NAME'
                                                             ,p_attr1_value   => 'RXH_CUSTOM.MI_EXT_PRESCRIPTION_XREF'
                                                             ,p_run_id     => g_run_id);
            -- Get current Percentage of errors
            g_perc := rxh_stage.a2r_migration_pkg.get_error_percentage@rxh_stg(t_rx_xref.COUNT,SQL%BULK_EXCEPTIONS.COUNT);
            FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              g_index_err := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
              BEGIN
              --
                 INSERT INTO (SELECT  atlas_order_no
                                     ,atlas_patient_id
                                     ,atlas_rx_with_location
                                     ,drug_route_ind
                                     ,processed_by_channel_ind
                                     ,rxhome_patient_id
                                     ,creation_date
                                     ,created_by
                                     ,update_date
                                     ,updated_by
                                FROM rxh_custom.mi_ext_prescription_xref)
                      VALUES t_rx_xref(g_index_err);
              EXCEPTION
                   WHEN OTHERS THEN
                        g_error_msg := SQLERRM;
              END;

              rxh_stage.a2r_log_pkg.log_bulk_errors_dtl@rxh_stg(p_hdr_id        => v_id
                                                               ,p_error_index   => SQL%BULK_EXCEPTIONS(j).ERROR_INDEX
                                                               ,p_error_code    => SQL%BULK_EXCEPTIONS(j).ERROR_CODE
                                                               ,p_error_msg     => g_error_msg --SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE)
                                                               ,p_attr1_name    => 'PATIENT_ID'
                                                               ,p_attr1_value   => t_rx_xref(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).rxhome_patient_id
                                                               ,p_run_id     => g_run_id);
              -- Delete the rowid value of the row that failed. This will prevent the row's processed flag from being updated.
              t_rx_xref_rowid(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)    := NULL;
               -- In order to reset existing values and review if the max value was reached
              g_error_msg := NULL;
              g_index_err := 0;
              EXIT WHEN g_perc > g_max_perc;
            END LOOP;
          END;
      END;

      rxh_stage.a2r_log_pkg.log_debug@rxh_stg(p_context     => v_context,
                                              p_error_msg => 'Records processed :' || t_rx_xref.COUNT || ' | Records inserted:' ||(t_rx_xref.COUNT - SQL%BULK_EXCEPTIONS.COUNT), p_run_id => g_run_id);

      FOR i IN 1 .. t_rx_xref.COUNT LOOP
        BEGIN
          UPDATE rxh_stage.mi_ext_prescription_xref@rxh_stg
             SET processed_flag = 'Y'
                ,processed_date = G_SYSDATE    -- CQ24083 jarcega Added
           WHERE ROWID = t_rx_xref_rowid(i);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      END LOOP;

      COMMIT;
    END LOOP;

    CLOSE prescription_xref_c;

    CLOSE prescription_xref_rowid_c;

    -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
    rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure successfully finished', p_run_id => g_run_id);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      IF prescription_xref_c%ISOPEN THEN
        CLOSE prescription_xref_c;
      END IF;

      IF prescription_xref_rowid_c%ISOPEN THEN
        CLOSE prescription_xref_rowid_c;
      END IF;

      -- CQ24083 jarcega Commented enable_triggers(t_triggers   => t_triggers);
      rxh_stage.a2r_log_pkg.log_info@rxh_stg(p_context     => v_context, p_error_msg => 'Procedure finished with errors', p_run_id => g_run_id);
      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context, p_error_code => SQLCODE, p_error_msg => SQLERRM, p_run_id => g_run_id);
      RAISE;
  END pull_mi_ext_prescription_xref;

   -- +================================================================================================+
   -- | Name:        A2R_CREATE_TASK_ASSESSMENT
   -- | Author:      Jose Corona
   -- | Description: Procedure to create therapy management assessments and task  from those assessments
   -- |              based on paremters stored in table RXH_STAGE.A2R_TPY_MGT_TASK_PARAMETERS  which is
   -- |              populated during migration. Assessmets and tasks will be generated bay calling
   -- |              existing RxHome procedures to generate assessement and task
   -- | CQ   :       23641
   -- | Notes:       CQ23641 - 16.3 - RFlore : Procedure Moved from a2r_trans_charts_pkg in order to avoid dblink ref.
   -- +================================================================================================+
   PROCEDURE a2r_create_task_assessment (P_RUN_ID IN NUMBER )IS
   -- CQ23013 - JCORONA - Removed default value

     t_table           adm.dm_atmdm_pkg.tSecCatIds ;
     t_record          adm.dm_atmdm_pkg.type_asm_items_attrbts_rec;
     v_retcode         NUMBER ;
     v_errors          VARCHAR2(4000);
     v_SessionID       NUMBER ;
     V_SessionAsmtId   NUMBER ;
     V_ICD9            THOT.PATIENT_DIAGNOSES.ICD9_CODE%TYPE;
     V_SECTION_ID      ADM.DM_ADMIN_SECTIONS.SECTION_ID%TYPE;
     V_CATEGORY_ID     adm.dm_admin_categories.CATEGORY_CODE%TYPE;
     V_THERAPY_D       THOT.THERAPY_TYPES.THERAPY_DESCR%TYPE ;
     ASSESMENT_EXCEP   EXCEPTION ;
     V_UPDATE_DATE     DATE := SYSDATE ;
     V_PARAMETERS_ID   NUMBER ;
     V_CONTEXT         VARCHAR2(200) := 'CREATE_TASK_ASSESSMENT';
     V_COUNTER         NUMBER := 0;

     V_USER           VARCHAR2(100):= USER;
     V_DATE           DATE         := SYSDATE;
     V_TASK_ID        NUMBER;
     V_THREAD         NUMBER;

     TYPE R_RECORD_PARAMETERS IS RECORD( WORKQUEUE_ID        THOT.FT_WORKQUEUES.WORKQUEUE_ID%TYPE
                                       , TASK_TYPE_ID        THOT.FT_TASK_TYPES.TASK_TYPE_ID%TYPE
                                       , TASK_STATUS_ID      THOT.FT_TASK_STATUSES.TASK_STATUS_ID%TYPE
                                       , TEAM_ASSIGNMENT_ID  THOT.FT_TEAM_ASSIGNMENTS.TEAM_ASSIGNMENT_ID%TYPE);

     R_RECORD      R_RECORD_PARAMETERS;
     R_RECORD_NULL R_RECORD_PARAMETERS ;
     v_comments VARCHAR2(1000) ;



      CURSOR C_PARAMETERS IS
       SELECT PARAMETERS_ID
            , RUN_ID
            , TASK_RN
            , TASK_TYPE_NAME
            , PATIENT_RN
            , RXH_PATIENT
            , ICD9_CODE
            , THERAPY_TYPE
            , STATUS
            , PROCESSED_FLAG
            , PROCESSED_DATE
            , WORKQUEUE_NAME
            , TASK_STATUS
            , TASK_TYPE
            , TEAM_DESCRIPTION
            , task_due_date
            , CREATED_BY
            , UPDATE_DATE
            , UPDATE_BY
            , CREATION_DATE
       FROM RXH_STAGE.A2R_TPY_MGT_TASK_PARAMETERS@rxh_stg
      WHERE  STATUS = 'ACTIVE'
        AND  RUN_ID =  P_RUN_ID
        AND  PROCESSED_FLAG = 'N'
        AND EXISTS (SELECT 1
                    FROM THOT.PATIENTS_TABLE
                    WHERE ID = RXH_PATIENT);

       V_QUERY_1 VARCHAR2(1000) := NULL;
       V_QUERY_2 VARCHAR2(1000) := NULL;

   BEGIN
         -- CQ23013 - JCORONA -  begin -Adding select
         /*SELECT MAX(thread_id)
           INTO V_THREAD
           FROM RXH_STAGE.a2r_threads@rxh_stg
          WHERE run_id = P_RUN_ID;*/
         -- CQ23013 - JCORONA - end

         RXH_STAGE.A2R_LOG_PKG.log_info@rxh_stg(p_context => V_CONTEXT, p_error_msg =>'PROCEDURE STARTED'
                                               ,p_attr1_name => 'START TIME'
                                               ,p_attr1_value => TO_CHAR(SYSDATE,'MM-DD-RRRR HH24:MI:SS')
                                               ,p_run_id => p_run_id);   -- CQ23013 - JCORONA - Thread id added

          SELECT ATTRIBUTE2
            INTO V_QUERY_1
            FROM  RXH_STAGE.A2R_MAPPING_VALUES@rxh_stg
           WHERE TABLE_NAME = 'TASK_ASSESSMENT'
             AND CONTEXT = 'DINAMIC_QUERY'
             AND FIELD = 'SECTION_DINAMIC_QUERY' ;

          EXECUTE IMMEDIATE V_QUERY_1
             INTO V_SECTION_ID;

           SELECT ATTRIBUTE2
             INTO V_QUERY_2
             FROM  RXH_STAGE.A2R_MAPPING_VALUES@rxh_stg
            WHERE TABLE_NAME = 'TASK_ASSESSMENT'
              AND CONTEXT    = 'DINAMIC_QUERY'
              AND FIELD      = 'CATEGORY_DINAMIC_QUERY';

           EXECUTE IMMEDIATE V_QUERY_2
              INTO V_CATEGORY_ID;


        FOR R_PARAMETERS IN C_PARAMETERS LOOP
            t_table.delete ;
            V_PARAMETERS_ID := R_PARAMETERS.PARAMETERS_ID;
            V_COUNTER       := V_COUNTER + 1 ;
            BEGIN
            SELECT THERAPY_DESCR
              INTO V_THERAPY_D
              FROM therapy_types
             WHERE therapy_type = R_PARAMETERS.THERAPY_TYPE;
            EXCEPTION
               WHEN OTHERS THEN
                  V_THERAPY_D := NULL;
                  rxh_stage.a2r_log_pkg.LOG_INFO@rxh_stg(p_context      => 'THERAPY DESC -'||v_context
                                                       , p_error_msg    => SQLCODE||' - '||SQLERRM
                                                       , p_attr1_name   => 'PARAMETER ID'
                                                       , p_attr1_value  => V_PARAMETERS_ID
                                                       , p_run_id => p_run_id); -- CQ23013 - JCORONA - Thread id added
            END;

            t_table(1).section_id    :=  V_SECTION_ID;
            t_table(1).category_id   :=  V_CATEGORY_ID;
            t_table(1).mtherapy_type :=  R_PARAMETERS.THERAPY_TYPE;
            t_table(1).mtherdescr    :=  V_THERAPY_D;
            t_table(1).micd9_code    :=  R_PARAMETERS.ICD9_CODE;

            t_record.ATTRIBUTE_SOURCE := NULL;
            t_record.ATTRIBUTE1       := NULL;
            t_record.ATTRIBUTE2       := NULL;
            t_record.ATTRIBUTE3       := NULL;
            t_record.ATTRIBUTE4       := NULL;
            t_record.ATTRIBUTE5       := NULL;
            t_record.ATTRIBUTE6       := NULL;
            t_record.ATTRIBUTE7       := NULL;
            t_record.ATTRIBUTE8       := NULL;
            t_record.ATTRIBUTE9       := NULL;
            t_record.ATTRIBUTE10      := NULL;


            ADM.DM_ATMDM_PKG.populate_assessment_items_new ( pseccatids            =>  t_table                   -- IN       tseccatids,
                                                           , ppatientid            =>  R_PARAMETERS.RXH_PATIENT  -- IN       NUMBER,
                                                           , ptherapytype          =>  R_PARAMETERS.THERAPY_TYPE -- IN       VARCHAR2,
                                                           , ptmident              =>  trunc(V_UPDATE_DATE)      -- IN       DATE,
                                                           , picd9code             =>  R_PARAMETERS.ICD9_CODE    -- IN       VARCHAR2,
                                                           , pcomments             =>  NULL                      -- IN       VARCHAR2,
                                                           , pexcluded             => 'N'                        -- IN       VARCHAR2,
                                                           , plocked               => 'N'                        -- IN       VARCHAR2,
                                                           , pexcluded_reason_id   =>  NULL                      -- IN       NUMBER,
                                                           , pAsm_items_attributes =>  t_record                  -- IN       type_asm_items_attrbts_rec,
                                                           , pSessionID            =>  v_SessionID               -- OUT      NUMBER,
                                                           , pSessionAsmtId        =>  V_SessionAsmtId           -- OUT      NUMBER,
                                                           , perror                =>  v_errors                  -- IN OUT   VARCHAR2,
                                                           , pretcode              =>  v_retcode                 -- IN OUT   NUMBER
                                                         );


            IF v_retcode > 0  OR v_SessionID IS NULL THEN
                 UPDATE RXH_STAGE.A2R_TPY_MGT_TASK_PARAMETERS@rxh_stg
                  SET  PROCESSED_FLAG  = 'F' -- THIS MEANS FAIL
                     , PROCESSED_DATE = G_SYSDATE    -- CQ24083 jarcega Added
                WHERE PARAMETERS_ID = V_PARAMETERS_ID;

                rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => 'CREATE SESSION ID -'||v_context
                                                      , p_error_code   => v_retcode
                                                      , p_error_msg    => v_errors
                                                      , p_attr1_name   => 'PARAMETER ID'
                                                      , p_attr1_value  => V_PARAMETERS_ID
                                                      , p_run_id => p_run_id); -- CQ23013 - JCORONA - Thread id added
            END IF ;


      IF v_SessionID IS NOT NULL THEN
               --dbms_output.put_line(v_SessionID||' - '||V_SessionAsmtId);
               BEGIN
                  UPDATE adm.Dm_Sessions
                     SET follow_up_days = 0
                   WHERE session_id = v_SessionID;

                 UPDATE Dm_Session_Assessments
                    SET follow_up_days = 0
                      , follow_up_date = trunc(V_UPDATE_DATE)
                  WHERE session_assessment_id = V_SessionAsmtId;


                 INSERT INTO ADM.DM_PATIENTS(SESSION_ID
                                                   , ID
                                                 , LAST
                                                , FIRST
                                                   , MI
                                             , NICKNAME
                                               , SOCSEC
                                                 , SEX
                                        , DATE_OF_BIRTH
                                             , SVCBR_ID
                                            , IV_DEVICE
                                              , SB_NAME
                                    , COMP_RX_SHIP_DATE
                                             , COMP_AGE
                                        , COMP_NICKNAME
                                   , COMP_RX_START_DATE
                                    , COMP_PHYSICIAN_ID
                                  , COMP_PHYSICIAN_NAME
                                           , GENERATION
                                               , CMP_ID
                                                , ADDR1
                                                , ADDR2
                                                 , CITY
                                                , STATE
                                                 , ZIP1
                                                 , ZIP2
                                               , COUNTY
                                              , COUNTRY
                                          , PHONE_TYPE1
                                              , PHONE1A
                                              , PHONE1B
                                              , PHONE1C
                                           , PHONE_EXT1
                                          , PHONE_TYPE2
                                              , PHONE2A
                                              , PHONE2B
                                              , PHONE2C
                                            , PHONE_EXT2
                                           , PHONE_TYPE3
                                               , PHONE3A
                                               , PHONE3B
                                               , PHONE3C
                                            , PHONE_EXT3
                                       , LANGUAGE_SPOKEN
                                              , RELIGION
                                            , RACIAL_MIX
                                              , INVOICER
                                          , MARKETING_ID
                                            , PRICING_ID
                                    , ADDR_LABEL_PRINTOK
                                                , MAPSCO
                                     , DELIVERY_INSTRUCT
                                       , MILITARY_STATUS
                                       , MILITARY_BRANCH
                                        , MARITAL_STATUS
                                        , STUDENT_STATUS
                                     , EMPLOYMENT_STATUS
                                          , MEDICAL_HIST
                                  , THERAPY_INSTRUCTIONS
                                       , RX_INSTRUCTIONS
                                      , THERAPY_DURATION
                                      , THERAPY_COMMENTS
                                           , RX_COMMENTS
                                  , PRIMARY_THERAPY_TYPE
                                     , NVISITS_FREQUENCY
                                               , IV_DATE
                                                   , POS
                                                   , TOS
                                       , EMPLOYMENT_FLAG
                                    , AUTO_ACCIDENT_FLAG
                                   , OTHER_ACCIDENT_FLAG
                                        , ACCIDENT_STATE
                                           , PT_CATEGORY
                                           , SMOKER_FLAG
                                               , CHARGES
                                        , PMT_DEDUCTIONS
                                        , ADJ_DEDUCTIONS
                                           , BALANCE_DUE
                                              , PAYMENTS
                                           , ADJUSTMENTS
                                                , UNPAID
                                         , LAST_PMT_DATE
                                   , PRESCRIPTIONS_COUNT
                                , SALES_MARKETING_STATUS
                                       , PHARMACY_STATUS
                                        , NURSING_STATUS
                                            , CSR_STATUS
                                             , COLLECTOR
                                       , OCCURRENCE_DATE
                                                  , TEAM
                                     , LAST_DUNNING_DATE
                             , LAST_DUNNING_PROFILE_NAME
                              , LAST_DUNNING_PROFILE_SEQ
                                   , LAST_DUNNING_UNPAID
                                  , DUNNING_PROFILE_NAME
                                          , DUNNING_FLAG
                                      , SPECIAL_INSTRUCT
                                      , COLL_ASSIGN_FLAG
                                       , ICR_ASSIGN_FLAG
                                   , LAST_DUR_CHECK_DATE
                                         , NVISITS_COUNT
                                         , PREGNANT_FLAG
                            , PROFILE_LAST_REVIEWED_USER
                            , PROFILE_LAST_REVIEWED_DATE
                                 , PROFILE_LAST_COMMENTS
                                             , NKDA_FLAG
                                       , FEP_CLIENT_FLAG
                                            , CREATED_BY
                                         , CREATION_DATE
                                            , UPDATED_BY
                                           , UPDATE_DATE
                                        , TM_UPDATE_DATE
                                       , TM_UPDATE_ROLE)  SELECT v_SessionID
                                                                , R_PARAMETERS.RXH_PATIENT
                                                                , tpt.LAST
                                                                , tpt.FIRST
                                                                , tpt.MI
                                                                , tpt.NICKNAME
                                                                , tpt.SOCSEC
                                                                , tpt.SEX
                                                                , tpt.DATE_OF_BIRTH
                                                                , tpt.SVCBR_ID
                                                                , tpt.IV_DEVICE
                                                                , tsb.NAME
                                                                , TO_CHAR(Dm_Atmdm_Pkg.get_start_shipping_date(R_PARAMETERS.RXH_PATIENT), 'MM/DD/YYYY')
                                                                , NULL
                                                                , tpt.NICKNAME
                                                                , TO_CHAR(Dm_Atmdm_Pkg.get_last_shipping_date(R_PARAMETERS.RXH_PATIENT), 'MM/DD/YYYY')
                                                                , NULL
                                                                , NULL
                                                                , tpt.GENERATION
                                                                , tpt.CMP_ID
                                                                , tpt.ADDR1
                                                                , tpt.ADDR2
                                                                , tpt.CITY
                                                                , tpt.STATE
                                                                , tpt.ZIP1
                                                                , tpt.ZIP2
                                                                , tpt.COUNTY
                                                                , tpt.COUNTRY
                                                                , tpt.PHONE_TYPE1
                                                                , tpt.PHONE1A
                                                                , tpt.PHONE1B
                                                                , tpt.PHONE1C
                                                                , tpt.PHONE_EXT1
                                                                , tpt.PHONE_TYPE2
                                                                , tpt.PHONE2A
                                                                , tpt.PHONE2B
                                                                , tpt.PHONE2C
                                                                , tpt.PHONE_EXT2
                                                                , tpt.PHONE_TYPE3
                                                                , tpt.PHONE3A
                                                                , tpt.PHONE3B
                                                                , tpt.PHONE3C
                                                                , tpt.PHONE_EXT3
                                                                , tpt.LANGUAGE_SPOKEN
                                                                , tpt.RELIGION
                                                                , tpt.RACIAL_MIX
                                                                , tpt.INVOICER
                                                                , tpt.MARKETING_ID
                                                                , tpt.PRICING_ID
                                                                , tpt.ADDR_LABEL_PRINTOK
                                                                , tpt.MAPSCO
                                                                , tpt.DELIVERY_INSTRUCT
                                                                , tpt.MILITARY_STATUS
                                                                , tpt.MILITARY_BRANCH
                                                                , tpt.MARITAL_STATUS
                                                                , tpt.STUDENT_STATUS
                                                                , tpt.EMPLOYMENT_STATUS
                                                                , tpt.MEDICAL_HIST
                                                                , tpt.THERAPY_INSTRUCTIONS
                                                                , tpt.RX_INSTRUCTIONS
                                                                , tpt.THERAPY_DURATION
                                                                , tpt.THERAPY_COMMENTS
                                                                , tpt.RX_COMMENTS
                                                                , tpt.PRIMARY_THERAPY_TYPE
                                                                , tpt.NVISITS_FREQUENCY
                                                                , tpt.IV_DATE
                                                                , tpt.POS
                                                                , tpt.TOS
                                                                , tpt.EMPLOYMENT_FLAG
                                                                , tpt.AUTO_ACCIDENT_FLAG
                                                                , tpt.OTHER_ACCIDENT_FLAG
                                                                , tpt.ACCIDENT_STATE
                                                                , tpt.PT_CATEGORY
                                                                , tpt.SMOKER_FLAG
                                                                , tpt.CHARGES
                                                                , tpt.PMT_DEDUCTIONS
                                                                , tpt.ADJ_DEDUCTIONS
                                                                , tpt.BALANCE_DUE
                                                                , tpt.PAYMENTS
                                                                , tpt.ADJUSTMENTS
                                                                , tpt.UNPAID
                                                                , tpt.LAST_PMT_DATE
                                                                , tpt.PRESCRIPTIONS_COUNT
                                                                , tpt.SALES_MARKETING_STATUS
                                                                , tpt.PHARMACY_STATUS
                                                                , tpt.NURSING_STATUS
                                                                , tpt.CSR_STATUS
                                                                , tpt.COLLECTOR
                                                                , tpt.OCCURRENCE_DATE
                                                                , tpt.TEAM
                                                                , tpt.LAST_DUNNING_DATE
                                                                , tpt.LAST_DUNNING_PROFILE_NAME
                                                                , tpt.LAST_DUNNING_PROFILE_SEQ
                                                                , tpt.LAST_DUNNING_UNPAID
                                                                , tpt.DUNNING_PROFILE_NAME
                                                                , tpt.DUNNING_FLAG
                                                                , tpt.SPECIAL_INSTRUCT
                                                                , tpt.COLL_ASSIGN_FLAG
                                                                , tpt.ICR_ASSIGN_FLAG
                                                                , tpt.LAST_DUR_CHECK_DATE
                                                                , tpt.NVISITS_COUNT
                                                                , tpt.PREGNANT_FLAG
                                                                , tpt.PROFILE_LAST_REVIEWED_USER
                                                                , tpt.PROFILE_LAST_REVIEWED_DATE
                                                                , tpt.PROFILE_LAST_COMMENTS
                                                                , tpt.NKDA_FLAG
                                                                , tpt.FEP_CLIENT_FLAG
                                                                , G_USER   -- CQ24083 jarcega Added
                                                                , V_UPDATE_DATE
                                                                , G_USER   -- CQ24083 jarcega Added
                                                                , V_UPDATE_DATE
                                                                , V_UPDATE_DATE
                                                                , null
                                                            FROM THOT.PATIENTS_TABLE tpt
                                                               , THOT.SERVICE_BRANCHES tsb
                                                           WHERE tpt.SVCBR_ID = tsb.ID
                                                             AND tpt.ID = R_PARAMETERS.RXH_PATIENT
                                                             AND NOT EXISTS (SELECT 1
                                                                               FROM ADM.DM_PATIENTS
                                                                              WHERE SESSION_ID = v_SessionID
                                                                                AND ID = R_PARAMETERS.RXH_PATIENT );
               EXCEPTION
                  WHEN OTHERS THEN
                     rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => 'UPDATE-INSERT '||v_context
                                                          , p_error_code   => SQLCODE
                                                          , p_error_msg    => SQLERRM
                                                          , p_attr1_name   => 'PARAMETER ID'
                                                          , p_attr1_value  => V_PARAMETERS_ID
                                                          , p_run_id => p_run_id); -- CQ23013 - JCORONA - Thread id added
               END ;


               R_RECORD := R_RECORD_NULL;

               BEGIN
                   V_COMMENTS := 'MIGRATED FROM ESI-CURASCRIPT ';
                   v_comments := v_comments || chr(10) || 'TASK NAME - (' || TO_CHAR(R_PARAMETERS.TASK_TYPE_NAME) || ')';
                   v_comments := v_comments || chr(10) || 'CREATED BY - (' || TO_CHAR(R_PARAMETERS.CREATED_BY)  || ')';
                   v_comments := v_comments || chr(10) || 'UPDATE DATE - (' ||TO_CHAR(R_PARAMETERS.UPDATE_DATE, 'MM/DD/YYYY HH24:MI:SS')  || ')';
                   v_comments := v_comments || chr(10) || 'UPDATE BY - (' ||  R_PARAMETERS.UPDATE_BY|| ')';
                   v_comments := v_comments || chr(10) || 'CREATION DATE - (' ||TO_CHAR(R_PARAMETERS.CREATION_DATE, 'MM/DD/YYYY HH24:MI:SS')|| ')';

                 SELECT WK.WORKQUEUE_ID,TT.TASK_TYPE_ID,TS.TASK_STATUS_ID,TA.TEAM_ASSIGNMENT_ID
                   INTO R_RECORD
                   FROM THOT.FT_WORKQUEUES WK
                       , THOT.FT_TASK_TYPES TT
                       , THOT.FT_TASK_STATUSES TS
                       , THOT.FT_TEAM_ASSIGNMENTS TA
                  WHERE   WK.WORKQUEUE_ID = TT.WORKQUEUE_ID
                    AND  WK.WORKQUEUE_ID = TS.WORKQUEUE_ID
                    AND  WK.WORKQUEUE_ID = TA.WORKQUEUE_ID
                    AND  WK.NAME         = R_PARAMETERS.WORKQUEUE_NAME--'THERAPY MGT'
                    AND  TT.TASK_TYPE    = R_PARAMETERS.TASK_TYPE --'OPEN ASSESSMENT'
                    AND  TS.TASK_STATUS  = R_PARAMETERS.TASK_STATUS --'OPEN'
                    AND  TA.DESCRIPTION  = R_PARAMETERS.TEAM_DESCRIPTION ;

                    SELECT thot.WQ_TASKS_S.NEXTVAL
                      INTO V_TASK_ID
                    FROM DUAL ;


                    INSERT INTO  THOT.WQ_TASKS VALUES ( V_TASK_ID                      --TASK_ID,
                                                        , 'ATMS Task'                  --DESCRIPTION,
                                                        , 'ATMS'                       --SOURCE_TYPE,
                                                        , NULL                         --SOURCE_ID,
                                                        , NULL                         --SVCBR_ID,
                                                        , R_PARAMETERS.RXH_PATIENT     --PATIENT_ID
                                                        , NULL                         --PRESCRIPTION_ID,
                                                        , NULL                         --REFILL_NO,
                                                        , R_RECORD.WORKQUEUE_ID        --WORKQUEUE_ID,
                                                        , R_RECORD.TEAM_ASSIGNMENT_ID  --TEAM_ASSIGNMENT_ID
                                                        , NULL                         --XUSER,
                                                        , v_comments                   --COMMENTS,
                                                        , NULL                         --LAST_STATUS_DATE,
                                                        , NULL                         --LOCKED_BY,
                                                        , NULL                         --LOCKED_DATE,
                                                        , R_PARAMETERS.task_due_date   --TASK_DATE,
                                                        , R_RECORD.TASK_STATUS_ID      --TASK_STATUS_ID,
                                                        , V_USER                       --CREATED_BY,
                                                        , V_DATE                       --CREATION_DATE,
                                                        , V_USER                       --UPDATED_BY,
                                                        , V_DATE                       --UPDATE_DATE,
                                                        , NULL                         --ASSIGNMENT_RULE_ID,
                                                        , v_SessionID                  --SOURCE_ID1,            SESSION ID
                                                        , R_PARAMETERS.RXH_PATIENT     --SOURCE_ID2,            PATIENT ID
                                                        , V_SessionAsmtId              --SOURCE_ID3,            ASSESSMENT ID
                                                        , NULL                         --TASK_STS_REASON_CODE_ID,
                                                        , R_RECORD.TASK_TYPE_ID        --TASK_TYPE_ID,
                                                        , NULL                         --TASK_PRIORITY,
                                                        , 'Y'                          --CHART_NOTES,
                                                        , 'N'                          --FEP_CLIENT_FLAG,
                                                        , NULL                         --THERAPY_TYPE,
                                                        , NULL                         --SESSION_ID,
                                                        , NULL                         --SESSION_ASSESSMENT_ID,
                                                        , 'M'                          --CONTACT_TYPE,
                                                        , NULL                         --COMPLETED_REQUIRED,
                                                        , NULL                         --WORK_ORDER_ID,
                                                        , NULL                         --ATTRIBUTE01,
                                                        , NULL                         --ATTRIBUTE02,
                                                        , NULL                         --ATTRIBUTE03,
                                                        , NULL                         --ATTRIBUTE04,
                                                        , NULL                         --ATTRIBUTE05,
                                                        , NULL                         --ATTRIBUTE06,
                                                        , NULL                         --ATTRIBUTE07,
                                                        , NULL                         --ATTRIBUTE08,
                                                        , NULL                         --ATTRIBUTE09,
                                                        , NULL                         --ATTRIBUTE10,
                                                        , NULL                         --INVOICE_ID,
                                                        , NULL                         --INVOICE_SEQ
                                                        ) ;



                    INSERT INTO    THOT.WQ_TASKS_HISTORY(  HISTORY_ID
						         , TASK_ID
							 , DESCRIPTION
							 , SOURCE_TYPE
							 , SOURCE_ID
							 , PATIENT_ID
                                                         , SVCBR_ID
                                                         , PRESCRIPTION_ID
							 , REFILL_NO
							 , WORKQUEUE_ID
							 , TEAM_ASSIGNMENT_ID
							 , XUSER
                                                         , ASSIGNMENT_RULE_ID
							 , COMMENTS
							 , LAST_STATUS_DATE
							 , LOCKED_BY
							 , LOCKED_DATE
							 , TASK_DATE
							 , TASK_STATUS_ID --
                                                         , CREATED_BY
                                                         , CREATION_DATE
                                                         , UPDATED_BY
                                                         , UPDATE_DATE
                                                         , SOURCE_ID1
                                                         , SOURCE_ID2
                                                         , SOURCE_ID3
                                                         , TASK_STS_REASON_CODE_ID
                                                         , TASK_TYPE_ID
                                                         , TASK_PRIORITY
                                                         , CHART_NOTES
                                                         , CONTACT_TYPE
	                                                                           	) VALUES (  THOT.WQ_TASKS_HISTORY_S.NEXTVAL
									                           , V_TASK_ID
                                                                                                   , 'ATMS Task'
                                                                                                   , 'ATMS'
									                           , NULL
									                           , R_PARAMETERS.RXH_PATIENT
									                           , NULL
									                           , NULL
									                           , NULL
									                           , R_RECORD.WORKQUEUE_ID
									                           , R_RECORD.TEAM_ASSIGNMENT_ID
									                           , NULL
									                           , NULL
									                           , v_comments --COMMENTS
									                           , NULL
									                           , NULL
									                           , NULL
									                           , R_PARAMETERS.task_due_date
									                           , R_RECORD.TASK_STATUS_ID
                                                                                                   , V_USER
									                           , V_DATE
									                           , V_USER
                                                                                                   , V_DATE
                                                                                                   , v_SessionID
                                                                                                   , R_PARAMETERS.RXH_PATIENT
                                                                                                   , V_SessionAsmtId
									                           , NULL
                                                                                                   , R_RECORD.TASK_TYPE_ID
                                                                                                   , NULL
									                           , 'Y'
									                           , 'M'
                                                                                        	);



               EXCEPTION
                   WHEN OTHERS THEN
                      UPDATE RXH_STAGE.A2R_TPY_MGT_TASK_PARAMETERS@rxh_stg
                         SET  PROCESSED_FLAG  = 'F'
                            , PROCESSED_DATE = G_SYSDATE    -- CQ24083 jarcega Added
                       WHERE PARAMETERS_ID = V_PARAMETERS_ID;

                      rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context
                                                            , p_error_code   => SQLCODE
                                                            , p_error_msg    => SQLERRM
                                                            , p_attr1_name   => 'PARAMETER ID'
                                                            , p_attr1_value  => V_PARAMETERS_ID
                                                            , p_run_id => p_run_id);  -- CQ23013 - JCORONA - Thread id added
               END ;


            END IF; -- IF SESSION ID IS NOT NULL


            UPDATE RXH_STAGE.A2R_TPY_MGT_TASK_PARAMETERS@rxh_stg
               SET  PROCESSED_FLAG  = 'Y'
                   , PROCESSED_DATE = G_SYSDATE    -- CQ24083 jarcega Added
                   , SESSION_ID     = v_SessionID
             WHERE PARAMETERS_ID    = V_PARAMETERS_ID
             AND  PROCESSED_FLAG  != 'F';


           IF mod(200, V_COUNTER) = 0 THEN
              -- THIS IS JUST TO AVOID TO DO A COMMIT OF MANY RECORDS
              COMMIT ;
           END IF ;

        END LOOP ;
        COMMIT ;
        RXH_STAGE.A2R_LOG_PKG.log_info@rxh_stg(p_context => V_CONTEXT, p_error_msg =>'PROCEDURE FINISHED'
                                       ,p_attr1_name   => 'START TIME'
                                       ,p_attr1_value  => TO_CHAR(SYSDATE,'MM-DD-RRRR HH24:MI:SS')
                                       ,p_run_id => p_run_id); -- CQ23013 - JCORONA - Thread id added
   EXCEPTION
      WHEN OTHERS THEN
          dbms_output.put_line(sqlerrm);
          rxh_stage.a2r_log_pkg.log_error@rxh_stg(p_context      => v_context
                                                , p_error_code   => SQLCODE
                                                , p_error_msg    => SQLERRM
                                                , p_attr1_name   => 'PARAMETER ID'
                                                , p_attr1_value  => V_PARAMETERS_ID
                                                , p_run_id => p_run_id ); -- CQ23013 - JCORONA - Thread id added

          RAISE;



   END a2r_create_task_assessment;

 -- +================================================================================================+
 -- | Name:        OS_TASK_ON_HOLD
 -- | Author:      Daniel Gamboa
 -- | Description: Procedure to send order schedule tasks to HOLD status, for patients in a2r_parameters
 -- | Date :       12/26/2013
 -- | CQ   :       24752
 -- | Notes:       CR135 - CQ24752 - ver. 17 - PLSQL block moved to package due to new code addded for M8
 -- +================================================================================================+
   PROCEDURE OS_TASK_ON_HOLD IS
      CURSOR C_TASKS --(P_DATE VARCHAR2) date is not needed as parameter now value is saved at table column charvalue1
      IS
        SELECT T.*
        FROM THOT.WQ_TASKS T ,
          thot.A2R_STD_PARAMETERS rw ,
          THOT.FT_TASK_STATUSES TS
        WHERE T.PATIENT_ID        = rw.NUMVALUE1
        AND rw.CONTEXT_VALUE      = 'OS_TASK_PATIENT_ID'
        AND rw.CONTEXT_LINE       = 1
        AND T.workqueue_id        = 14
        AND T.TEAM_ASSIGNMENT_ID IN (11285, 3131,2907 ,23400 ,10583,10585 ,10586 ,10587 ,10588 ,10589 ,21903 ,21902 ,2910 ,27802 ,2911 ,33013 ,2918 ,34816 ,4364 ,26505,2920 ,3133 ,24700 ,2922 ,26507 ,34015 ,34818 ,3159 ,2913 ,2923 ,26199 ,18994 ,18995)
        AND TS.WORKQUEUE_ID       = T.WORKQUEUE_ID
        AND ts.task_status_id     = T.task_status_id
        AND TS.TASK_STATUS NOT   IN ('COMPLETED','HOLD')
        AND EXISTS
          (SELECT 1
             FROM THOT.Prescriptions_table
            WHERE --RX_WRITTEN_DATE <= TO_DATE(P_DATE ,'MM/DD/YYYY') date is not needed as parameter now value is saved at table column charvalue1 - M8 - CR135 - DGamboa
                  RX_WRITTEN_DATE <= TO_DATE(rw.CHARVALUE1,'MM/DD/YYYY') -- M8 - CR135 - DGamboa - Change date check againts charvalue1 instead of
              AND REFILL_NO          = T.REFILL_NO
              AND SVCBR_ID           = T.SVCBR_ID
              AND PRESCRIPTION_ID    = T.PRESCRIPTION_ID
          )
        AND T.REFILL_NO = 0;
    TYPE task_tab
    IS
      TABLE OF C_TASKS%rowtype;
      t_tasks task_tab;
      v_task_status_id THOT.WQ_TASKS.TASK_status_id%type;
      v_date DATE          := SYSDATE;
      v_user      VARCHAR2(100) := USER;
      V_PROD_DATE VARCHAR2(30) ;
    BEGIN
      SELECT ts.task_status_id
      INTO v_task_status_id
      FROM THOT.FT_TASK_STATUSES TS
      WHERE ts.task_status LIKE 'HOLD'
      AND ts.workqueue_id = 14;
      /* [Start - M8 - CR135 - DGamboa - 12/26/2013
      SELECT CHARVALUE1
      INTO V_PROD_DATE
      FROM THOT.A2R_STD_PARAMETERS
      WHERE context_value = 'MIGRATION PROD DATE'
      AND CONTEXT_LINE    = 1;
      OPEN C_TASKS(V_PROD_DATE);
      - M8 - CR135 - DGamboa - 12/26/2013 - End ] */
      OPEN C_TASKS;
      FETCH C_TASKS BULK COLLECT INTO t_tasks;
      FOR i IN 1..t_tasks.COUNT
      LOOP
        UPDATE THOT.WQ_TASKS
        SET task_status_id = v_task_status_id ,
          comments         = 'CSP Migration OS Task - Now Ready To Schedule at Patients Request',
          UPDATED_BY       = v_user ,
          UPDATE_DATE      = v_date
        WHERE task_id      = t_tasks(i).task_id ;
        INSERT
        INTO THOT.WQ_TASKS_HISTORY
          (
            HISTORY_ID,
            TASK_ID ,
            DESCRIPTION,
            SOURCE_TYPE,
            SOURCE_ID ,
            PATIENT_ID,
            SVCBR_ID ,
            PRESCRIPTION_ID,
            REFILL_NO,
            WORKQUEUE_ID ,
            TEAM_ASSIGNMENT_ID,
            XUSER ,
            ASSIGNMENT_RULE_ID ,
            COMMENTS ,
            LAST_STATUS_DATE,
            LOCKED_BY,
            LOCKED_DATE,
            TASK_DATE ,
            TASK_STATUS_ID ,
            CREATED_BY,
            CREATION_DATE,
            UPDATED_BY,
            UPDATE_DATE ,
            SOURCE_ID1 ,
            SOURCE_ID2,
            SOURCE_ID3,
            TASK_STS_REASON_CODE_ID ,
            TASK_TYPE_ID ,
            TASK_PRIORITY ,
            CHART_NOTES ,
            CONTACT_TYPE
          )
          VALUES
          (
            THOT.WQ_TASKS_HISTORY_S.NEXTVAL ,
            t_tasks(i).TASK_ID,
            t_tasks(i).DESCRIPTION,
            t_tasks(i).SOURCE_TYPE,
            t_tasks(i).SOURCE_ID,
            t_tasks(i).PATIENT_ID ,
            t_tasks(i).SVCBR_ID ,
            t_tasks(i).PRESCRIPTION_ID,
            t_tasks(i).REFILL_NO ,
            t_tasks(i).WORKQUEUE_ID ,
            t_tasks(i).TEAM_ASSIGNMENT_ID,
            t_tasks(i).XUSER ,
            t_tasks(i).ASSIGNMENT_RULE_ID,
            'CSP Migration OS Task - Now Ready To Schedule at Patients Request',
            t_tasks(i).LAST_STATUS_DATE ,
            t_tasks(i).LOCKED_BY ,
            t_tasks(i).LOCKED_DATE,
            t_tasks(i).TASK_DATE,
            v_task_status_id ,
            v_user,
            v_date ,
            v_user,
            v_date,
            t_tasks(i).SOURCE_ID1 ,
            t_tasks(i).SOURCE_ID2,
            t_tasks(i).SOURCE_ID3,
            t_tasks(i).TASK_STS_REASON_CODE_ID,
            t_tasks(i).TASK_TYPE_ID,
            t_tasks(i).TASK_PRIORITY,
            t_tasks(i).CHART_NOTES,
            t_tasks(i).CONTACT_TYPE
          );
        COMMIT;
      END LOOP;
      CLOSE C_TASKS;
    EXCEPTION
    WHEN OTHERS THEN
      thot.ft_message_log_pkg.insert_row(p_context => 'DM TIER6 TASK ON HOLD' ,p_source_process_id => 2 ,p_msg_type => 'ERROR' ,p_message => sqlerrm);
      ROLLBACK;
      IF C_TASKS%ISOPEN THEN
        CLOSE C_TASKS;
      END IF;
    END;

 -- +================================================================================================+
 -- | Name:        PRECALL_TASK_ON_HOLD
 -- | Author:      Daniel Gamboa
 -- | Description: Procedure to send order schedule tasks to HOLD status, for patients in a2r_parameters
 -- | Date :       12/26/2013
 -- | CQ   :       24752
 -- | Notes:       CR 134 - CQ24752 - ver. 17 - PLSQL block moved to package due to new code addded for M8
 -- +================================================================================================+
   PROCEDURE precall_task_on_hold IS
     CURSOR C_TASKS  IS
      SELECT      T.*
        FROM THOT.WQ_TASKS  T
           , THOT.A2R_STD_PARAMETERS rw
           , THOT.FT_TASK_STATUSES TS
       WHERE T.PATIENT_ID = rw.NUMVALUE1
         AND rw.CONTEXT_VALUE = 'PRECALL TASK ON HOLD'
         AND rw.CONTEXT_LINE  = 1
         AND T.workqueue_id   = 2 -- Precall Task
         AND T.TEAM_ASSIGNMENT_ID IN (10783 ,6165 ,58014 ,8783 ,2948 ,27402 ,2555 ,2321 ,2546 ,2325 ,2269 ,2266 ,2424 ,2273 ,2828 ,5262 ,13385 ,2419 ,2027
                                     ,2452 ,36014 ,26201 ,11284 ,17389 ,3130 ,23399 ,2885 ,2886 ,10590 ,10592 ,10593 ,10594 ,10595 ,10596 ,40014 ,21899 ,21901
                                     ,21900 ,27803 ,2888 ,2889 ,17387 ,2892 ,2893 ,33012 ,2897 ,2898 ,34815 ,35414 ,35415 ,26504 ,4362 ,17388 ,2900 ,2901 ,23199
                                     ,17587 ,3132 ,2902 ,2904 ,26506 ,36414 ,34014 ,37514 ,34817 ,3158 ,53014 ,2894 ,2895 ,36215 ,3160 ,2760 ,55014 ,39220 ,23802
                                     ,27602 ,2025 ,27807 ,2899 ,2903 ,3237 ,2891 ,26502 ,18990 ,18993 ,18991 )
         AND TS.WORKQUEUE_ID  = T.WORKQUEUE_ID
         AND ts.task_status_id = T.task_status_id
         AND TS.TASK_STATUS NOT IN ('COMPLETED','HOLD')
         AND EXISTS ( SELECT 1
                        FROM THOT.Prescriptions_table
                       WHERE RX_WRITTEN_DATE <= TO_DATE( rw.CHARVALUE1,'MM/DD/YYYY')
                         AND REFILL_NO        = T.REFILL_NO
                         AND SVCBR_ID         = T.SVCBR_ID
                         AND PRESCRIPTION_ID  = T.PRESCRIPTION_ID
                       -- M8 - CR134 - DGamboa - 12/24/2013 - Exclude Active or profile Rxs
                         AND (RX_STATUS NOT IN ('ACTIVE','PROFILE')
                              OR
                              PRESCR_SHIP_DATE  <= TO_DATE( rw.CHARVALUE1,'MM/DD/YYYY') -- hold task for Rxs shippped before prod date
                             )
                       -- M8 - CR134 - DGamboa - 12/24/2013 - Exclude Active or profile Rxs
                   );
   --M8 - DGAMBOA - 12/24/2013 - CR134 - Main cursor group by RX and patient for active ones
     CURSOR EXCLUDE_PAT IS
        SELECT count(RX_NO) Total_RX, patient_id
          FROM (SELECT  T.svcbr_id||'-'|| T.prescription_id||'-'|| T.refill_no RX_NO, T.patient_id
                  FROM THOT.WQ_TASKS  T
                     , THOT.A2R_STD_PARAMETERS rw
                     , THOT.FT_TASK_STATUSES TS
                 WHERE T.PATIENT_ID = rw.NUMVALUE1
                   AND rw.CONTEXT_VALUE = 'PRECALL TASK ON HOLD'
                   AND rw.CONTEXT_LINE  = 1
                   AND T.workqueue_id   = 2 -- Precall Task
                   AND T.TEAM_ASSIGNMENT_ID IN (10783 ,6165 ,58014 ,8783 ,2948 ,27402 ,2555 ,2321 ,2546 ,2325 ,2269 ,2266 ,2424 ,2273 ,2828 ,5262 ,13385 ,2419 ,2027
                                               ,2452 ,36014 ,26201 ,11284 ,17389 ,3130 ,23399 ,2885 ,2886 ,10590 ,10592 ,10593 ,10594 ,10595 ,10596 ,40014 ,21899 ,21901
                                              ,21900 ,27803 ,2888 ,2889 ,17387 ,2892 ,2893 ,33012 ,2897 ,2898 ,34815 ,35414 ,35415 ,26504 ,4362 ,17388 ,2900 ,2901 ,23199
                                              ,17587 ,3132 ,2902 ,2904 ,26506 ,36414 ,34014 ,37514 ,34817 ,3158 ,53014 ,2894 ,2895 ,36215 ,3160 ,2760 ,55014 ,39220 ,23802
                                              ,27602 ,2025 ,27807 ,2899 ,2903 ,3237 ,2891 ,26502 ,18990 ,18993 ,18991 )
                   AND TS.WORKQUEUE_ID  = T.WORKQUEUE_ID
                   AND ts.task_status_id = T.task_status_id
                   AND TS.TASK_STATUS NOT IN ('COMPLETED','HOLD')
                   AND EXISTS ( SELECT 1
                                  FROM THOT.Prescriptions_table
                                 WHERE RX_WRITTEN_DATE <= TO_DATE( rw.CHARVALUE1,'MM/DD/YYYY')
                                   AND REFILL_NO        = T.REFILL_NO
                                   AND SVCBR_ID         = T.SVCBR_ID
                                   AND PRESCRIPTION_ID  = T.PRESCRIPTION_ID
                                   AND (RX_STATUS IN ('ACTIVE','PROFILE')
                                       AND
                                       PRESCR_SHIP_DATE  > TO_DATE( rw.CHARVALUE1,'MM/DD/YYYY')
                                       )
                              )
                ) exclude_cadidates
        Group by patient_id    ;

      TYPE EXCLUDE_PAT_TAB IS TABLE OF EXCLUDE_PAT%rowtype;
      t_exc_pat EXCLUDE_PAT_TAB;
      v_context_exc   varchar(30) := 'DM PRECALL EXCLUDE PATIENTS';
      --M8 - DGAMBOA - 12/24/2013 - CR134 - Cursor and variable declaration ends

      TYPE task_tab IS TABLE OF C_TASKS%rowtype;
      t_tasks  task_tab;
      v_task_status_id  THOT.WQ_TASKS.TASK_status_id%type;
      v_date      DATE          := SYSDATE;
      v_user      VARCHAR(100)  := USER;
      v_context   VARCHAR2(30)  := 'DM PRECALL TASK ON HOLD';
      v_comments  VARCHAR2(100) := 'CSP Migration Precall Task - Now Ready To Schedule at Patients Request';

   BEGIN
      -- [ M8 - DGAMBOA - 12/24/2013 - CR134 - Logic to remove from to hold list, applied before sending to hold the tasks
      OPEN EXCLUDE_PAT;
      FETCH EXCLUDE_PAT BULK COLLECT INTO t_exc_pat;
      FOR j IN 1..t_exc_pat.count LOOP
         BEGIN
            IF t_exc_pat(j).Total_RX = 1 THEN
            -- If only one ACTIVE or PROFILE RX EXIST then the patient id is deleted from the TO HOLD list
               DELETE THOT.A2R_STD_PARAMETERS
                WHERE context_value = 'PRECALL TASK ON HOLD'
                  AND t_exc_pat(j).patient_id = numvalue1;

            END IF;

            COMMIT;
         EXCEPTION
            WHEN OTHERS THEN
               thot.ft_message_log_pkg.insert_row(p_context           => v_context_exc
                                                 ,p_source_process_id => 2
                                                 ,p_msg_type          => 'ERROR'
                                                 ,p_message           => 'PATIENT ID : '||t_exc_pat(j).patient_id||' - '||sqlerrm);
               ROLLBACK;
         END;
      END LOOP;
      CLOSE EXCLUDE_PAT;
      --M8 - DGAMBOA - 12/24/2013 - CR134 - Exclude patients from TO HOLD list - End ]

      SELECT ts.task_status_id
        INTO v_task_status_id
        FROM THOT.FT_TASK_STATUSES TS
       WHERE ts.task_status = 'HOLD'
         AND ts.workqueue_id = 2;

      OPEN C_TASKS;
      FETCH C_TASKS BULK COLLECT INTO t_tasks;
      FOR i IN 1..t_tasks.COUNT LOOP
          BEGIN
             UPDATE THOT.WQ_TASKS
                SET task_status_id = v_task_status_id
                  , comments       = v_comments
                  , UPDATED_BY     = v_user
                  , UPDATE_DATE    = v_date
              WHERE task_id        = t_tasks(i).task_id ;

             INSERT INTO THOT.WQ_TASKS_HISTORY( HISTORY_ID, TASK_ID , DESCRIPTION, SOURCE_TYPE, SOURCE_ID , PATIENT_ID, SVCBR_ID , PRESCRIPTION_ID, REFILL_NO, WORKQUEUE_ID , TEAM_ASSIGNMENT_ID, XUSER
                                              , ASSIGNMENT_RULE_ID , COMMENTS , LAST_STATUS_DATE, LOCKED_BY, LOCKED_DATE, TASK_DATE , TASK_STATUS_ID , CREATED_BY, CREATION_DATE, UPDATED_BY, UPDATE_DATE
                                              , SOURCE_ID1 , SOURCE_ID2, SOURCE_ID3, TASK_STS_REASON_CODE_ID , TASK_TYPE_ID , TASK_PRIORITY  , CHART_NOTES , CONTACT_TYPE
                                              ) VALUES ( THOT.WQ_TASKS_HISTORY_S.NEXTVAL  , t_tasks(i).TASK_ID, t_tasks(i).DESCRIPTION, t_tasks(i).SOURCE_TYPE, t_tasks(i).SOURCE_ID, t_tasks(i).PATIENT_ID
                                                       , t_tasks(i).SVCBR_ID , t_tasks(i).PRESCRIPTION_ID, t_tasks(i).REFILL_NO  , t_tasks(i).WORKQUEUE_ID , t_tasks(i).TEAM_ASSIGNMENT_ID, t_tasks(i).XUSER
                                                       , t_tasks(i).ASSIGNMENT_RULE_ID, v_comments, t_tasks(i).LAST_STATUS_DATE , t_tasks(i).LOCKED_BY
                                                       , t_tasks(i).LOCKED_DATE, t_tasks(i).TASK_DATE, v_task_status_id , v_user, v_date , v_user, v_date, t_tasks(i).SOURCE_ID1
                                                       , t_tasks(i).SOURCE_ID2, t_tasks(i).SOURCE_ID3, t_tasks(i).TASK_STS_REASON_CODE_ID, t_tasks(i).TASK_TYPE_ID, t_tasks(i).TASK_PRIORITY, t_tasks(i).CHART_NOTES, t_tasks(i).CONTACT_TYPE );
             COMMIT;
          EXCEPTION
             WHEN OTHERS THEN
                thot.ft_message_log_pkg.insert_row(p_context           => v_context
                                                  ,p_source_process_id => 2
                                                  ,p_msg_type          => 'ERROR'
                                                  ,p_message           => 'TASK ID : '||t_tasks(i).task_id||' - '||sqlerrm);
                ROLLBACK;
          END;

       END LOOP;
       CLOSE C_TASKS;
       COMMIT;
   EXCEPTION
      WHEN OTHERS THEN
         thot.ft_message_log_pkg.insert_row(p_context           => v_context
                                           ,p_source_process_id => 2
                                           ,p_msg_type          => 'ERROR'
                                           ,p_message           => sqlerrm);
         ROLLBACK;
         IF C_TASKS%ISOPEN THEN
           CLOSE C_TASKS;
         END IF;
         IF EXCLUDE_PAT%ISOPEN THEN
           CLOSE EXCLUDE_PAT;
         END IF;
   END precall_task_on_hold;
END a2r_migration_pull_pkg;
