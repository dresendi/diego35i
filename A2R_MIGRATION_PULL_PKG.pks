create or replace PACKAGE      A2R_MIGRATION_PULL_PKG AS


  bulk_limit   NUMBER        := 10000;
  G_SYSDATE    DATE          := SYSDATE;      -- CQ24083 jarcega Added
  G_USER       VARCHAR2(100) := USER;      -- CQ24083 jarcega Added
  bulk_errors  EXCEPTION;
  PRAGMA EXCEPTION_INIT(bulk_errors, -24381);

  TYPE triggers_tab IS TABLE OF rxh_stage.a2r_triggers@rxh_stg%ROWTYPE;

  PROCEDURE run_migration_pull (p_thread_id NUMBER);

  PROCEDURE disable_triggers(p_table_owner rxh_stage.a2r_triggers.table_owner@rxh_stg%TYPE, p_table_name rxh_stage.a2r_triggers.table_name@rxh_stg%TYPE, t_triggers OUT triggers_tab);

  PROCEDURE enable_triggers(t_triggers triggers_tab);

  --========= USERS ================
  PROCEDURE pull_a2r_users_xref;

  PROCEDURE pull_users;

  PROCEDURE pull_pharmacists;

  PROCEDURE pull_technicians;

  --========  USER FLAGS =============
  PROCEDURE pull_user_flag_groups;

  PROCEDURE pull_user_flag_types;

  PROCEDURE pull_pt_user_flag_types;

  --========== PHYSICIANS ============
  PROCEDURE pull_physicians;

  PROCEDURE pull_attributes;

  PROCEDURE pull_physician_dea;

  PROCEDURE pull_npi_table;

  PROCEDURE pull_phy_addresses;

  PROCEDURE pull_phy_phones;

  --========== PATIENTS =============
  PROCEDURE pull_patients_table;

  PROCEDURE pull_mi_patients_xref; --CQ22731 New procedure for TIER 3

  PROCEDURE pull_addresses;

  PROCEDURE pull_phones;

  PROCEDURE pull_patient_relations;

  PROCEDURE pull_patient_diagnoses;

  PROCEDURE pull_patient_drug_allergies;

  PROCEDURE pull_patient_nondrug_allergies;

  PROCEDURE pull_patient_refsource;

  PROCEDURE pull_patient_physician;

  PROCEDURE pull_patient_measurement;

  PROCEDURE pull_patient_status;

  PROCEDURE pull_patient_therapies;

  PROCEDURE pull_charts;

  PROCEDURE pull_assigned_ids;

  PROCEDURE pull_med_profile;

  PROCEDURE pull_credit_cards;

  PROCEDURE pull_ms_sddb;

  PROCEDURE pull_cc_pt_details;

  --========= PATIENTS INSURANCE =======
  PROCEDURE pull_patient_insurance;

  PROCEDURE pull_auth;

  PROCEDURE pull_auth_detail;

  PROCEDURE pull_subscriber_employer;

  --========= PRESCRIPTIONS ==========
  PROCEDURE pull_mi_ext_prescription_xref;  --CQ22731 New procedure for TIER 3

  PROCEDURE pull_prescriptions_table;

  PROCEDURE pull_prescription_drugs;

  PROCEDURE pull_noncompounded_detail;

  PROCEDURE pull_compounding_record;

  PROCEDURE pull_compounding_detail;

  PROCEDURE pull_ancillary_prescriptions;

  PROCEDURE pull_shipments;

  PROCEDURE pull_shipment_items;

  PROCEDURE pull_shipment_rxs;

  PROCEDURE pull_shipping;

  PROCEDURE pull_shipping_dtl;

  PROCEDURE pull_shipping_dtl_rxs;

  PROCEDURE pull_audit_events;

  PROCEDURE pull_rx_audit;

  PROCEDURE pull_rx_refill_rules;

  PROCEDURE pull_ext_sys_pkey_xref;

  PROCEDURE complete_pull_rx;

--========== ICSLOCS =======
  PROCEDURE pull_icslocs;

  --=========IMS_DOCUMENTS =========
  PROCEDURE pull_ims_documents;

--=========POM===============
  PROCEDURE pull_wq_tasks;

  PROCEDURE pull_wq_tasks_history;
  -- [ Tier 5 - RFlore - Moved from a2r_trans_charts_pkg
  PROCEDURE a2r_create_task_assessment (P_RUN_ID IN NUMBER );
  -- end]
  -- [ M8 - DGamboa - ver 8.0 - 12/26/2013 - CQ24752
  PROCEDURE os_task_on_hold;
  -- end]
  -- [ M8 - DGamboa - ver 8.1 - 12/26/2013 - CQ24752
  PROCEDURE precall_task_on_hold;
  -- end]
END a2r_migration_pull_pkg;
