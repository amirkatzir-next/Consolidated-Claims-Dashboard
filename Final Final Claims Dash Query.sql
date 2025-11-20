DROP TABLE IF EXISTS db_data_science.amir_claims_monthly_combined_emergence CASCADE;
CREATE TABLE db_data_science.amir_claims_monthly_combined_emergence AS (

WITH
    Inputs as (
        SELECT
            25000::double precision as cap_a, 50000::double precision as cap_b, 100000::double precision as cap_c,
            250000::double precision as cap_d, 500000::double precision as cap_e, 750000::double precision as cap_f,
            1000000::double precision as cap_g
    ),

    Claims_Data AS (
        WITH
            Legal_Data as (
                SELECT
                    claim_number,
                    transaction_month,
                    MAX(in_suit_flag) as in_suit_flag,
                    MAX(attorney_rep_flag) as attorney_rep_flag
                FROM (
                    SELECT
                        f.claim_number,
                        CAST(date_trunc('month', f.date) AS DATE) as transaction_month,
                        CASE WHEN s.legal_status IN ('attorney rep', 'in suit') THEN 1 ELSE 0 END as attorney_rep_flag,
                        CASE WHEN s.legal_status = 'in suit' THEN 1 ELSE 0 END as in_suit_flag,
                        ROW_NUMBER() OVER (PARTITION BY f.claim_number, f.exposure_id, date_trunc('month', f.date) ORDER BY f.date DESC) as rn
                    FROM dwh.all_claims_financial_changes_ds f
                    LEFT JOIN db_data_science.claims_slowly_changing_dim_legal_status s
                        ON s.exposure_id = f.exposure_id
                        AND f.date BETWEEN date_trunc('day', s.start_datetime) AND dateadd(day, -1, date_trunc('day', s.end_datetime))
                )
                WHERE rn = 1
                GROUP BY claim_number, transaction_month
            ),
            Claim_Level_Data as (
                SELECT
                    lrd.policy_reference, lrd.claim_number,
                    lrd.lob, -- *** REVERTED TO ORIGINAL ***
                    lrd.cob_name, lrd.cob_group,
                    case
                        when lrd.cob_group in ('Food & beverage', 'Food & beverage - deprecated') then 'Food & Beverage'
                        when lrd.cob_group in ('Retail', 'Retail - deprecated') then 'Retail'
                        when lrd.cob_group in ('Professional Services', 'Professional Services - deprecated') then 'Professional Services'
                        when lrd.cob_group = 'Day Care' then 'Day Care'
                        when lrd.cob_name in ('Tree Services', 'Welding, Cutting and Metal Frame Erection') then 'Construction'
                        when lrd.cob_group = 'Construction' then 'Construction'
                        else 'All Other'
                    end as cob_grouping,
                    case
                        when agencytable.agency_type IN ('AP Intego', 'APIntego') then 'AP Intego'
                        when channel IN ('AP Intego', 'APIntego') then 'AP Intego'
                        else channel
                    end as channel,
                    lrd.paygo_indicator, lrd.state, losscausetable.loss_cause_type_name,
                    lrd.accident_month::date,
                    extract(year from lrd.accident_month) as AY,
                    extract(year from lrd.policy_start_date) as PY,
                    extract(year from lrd.report_month) as RY,
                    lrd.ay_age, lrd.py_age,

                    -- This is your fix for negative ry_age on LOSSES. This is correct.
                    CASE
                        WHEN lrd.ry_age < 0 THEN lrd.ay_age   -- If < 0, use ay_age
                        ELSE lrd.ry_age       -- Otherwise, use the original ry_age
                    END AS ry_age,

                    lrd.am_age, lrd.transaction_month::date, lrd.coverage,
                    coalesce(ld.in_suit_flag, 0) as in_suit_flag,
                    coalesce(ld.attorney_rep_flag, 0) as attorney_rep_flag,
                    lrd.food_and_bev_express_ind,
                    agencytable.agency_type,
                    lrd.renewal_tag,
                    -- *** NEW COLUMN ADDED ***
                    CASE WHEN lrd.is_bop_policy = 1 THEN 'BOP Policy' ELSE 'Not BOP Policy' END as bop_policy_flag,
                    sum(paid_loss) as paid_loss,
                    sum(incurred_loss) as reported_loss, -- This is the field to be carried through
                    sum(paid_dcce) as paid_dcce,
                    sum(paid_aoe) as paid_aoe, sum(paid_dcce + paid_aoe) as paid_alae, sum(incurred_dcce) as reported_dcce,
                    sum(incurred_aoe) as reported_aoe, sum(incurred_dcce + incurred_aoe) as reported_alae,
                    sum(coalesce(paid_loss, 0) + coalesce(paid_subro, 0) + coalesce(paid_salvage, 0)) as paid_loss_ss,
                    sum(coalesce(incurred_loss, 0) + coalesce(paid_subro, 0) + coalesce(paid_salvage, 0)) as reported_loss_ss,
                    sum(coalesce(paid_loss, 0) + coalesce(paid_DCCE, 0) + coalesce(paid_aoe, 0) + coalesce(paid_subro, 0) + coalesce(paid_salvage, 0)) as paid_lossandALAE_ss,
                    sum(coalesce(incurred_loss, 0) + coalesce(incurred_DCCE, 0) + coalesce(incurred_aoe, 0) + coalesce(paid_subro, 0) + coalesce(paid_salvage, 0)) as reported_lossandALAE_ss,
                    coalesce(sum(coalesce(incurred_loss, 0) + coalesce(incurred_DCCE, 0) + coalesce(incurred_aoe, 0) + coalesce(paid_subro, 0) + coalesce(paid_salvage, 0)),0) - coalesce(sum(coalesce(paid_loss, 0) + coalesce(paid_DCCE, 0) + coalesce(paid_aoe, 0) + coalesce(paid_subro, 0) + coalesce(paid_salvage, 0)),0) as case_lossandALAE,
                    coalesce(sum(coalesce(incurred_loss, 0) + coalesce(paid_subro, 0) + coalesce(paid_salvage, 0)),0) - coalesce(sum(coalesce(paid_loss, 0) + coalesce(paid_subro, 0) + coalesce(paid_salvage, 0)),0) as case_loss,
                    sum(incurred_indemnity_loss) as reported_indemnity_loss,
                    sum(incurred_medical_loss) as reported_medical_loss,
                    sum(coalesce(paid_salvage, 0) + coalesce(paid_subro, 0)) as total_ss,
                    sum(coalesce(incurred_loss, 0) + coalesce(incurred_dcce, 0) + coalesce(incurred_aoe, 0)) as total_incurred_xss

                FROM db_data_science.ultimate_lrd as lrd
                LEFT JOIN Legal_Data ld ON lrd.claim_number = ld.claim_number AND lrd.transaction_month = ld.transaction_month
                LEFT JOIN (SELECT policy_reference, agency_type FROM (SELECT policy_reference, agency_type, transaction_month, ROW_NUMBER() OVER (PARTITION BY policy_reference ORDER BY transaction_month desc, agency_type desc) AS rn FROM db_data_science.ultimate_lrd) AS agencytable_ordered_by_date WHERE rn = 1) as agencytable ON agencytable.policy_reference = lrd.policy_reference
                LEFT JOIN (SELECT claim_number, loss_cause_type_name FROM (SELECT claim_number, loss_cause_type_name, ROW_NUMBER() OVER (PARTITION BY claim_number ORDER BY transaction_month DESC, total_incurred DESC, loss_cause_type_name DESC) as rn2 FROM (SELECT claim_number, loss_cause_type_name, transaction_month, SUM(incurred_loss_and_alae_ss) as total_incurred FROM db_data_science.ultimate_lrd GROUP BY 1, 2, 3) sub) ordered WHERE rn2 = 1) as losscausetable ON losscausetable.claim_number = lrd.claim_number
                WHERE lrd.ay_age > 0 AND lrd.claim_number IS NOT NULL
                -- *** REVERTED TO ORIGINAL ***
                AND lrd.lob IN ('IM', 'CA', 'WC', 'PL', 'CP', 'GL')
                -- *** UPDATED GROUP BY (added 26) ***
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
            ),
            Claim_Manipulation_Prep as (
                SELECT
                    a.*, i.*,
                    CASE WHEN a.reported_lossandALAE_ss > i.cap_a THEN i.cap_a ELSE a.reported_lossandALAE_ss END as capped_a_reported_lossandALAE_ss, CASE WHEN a.reported_lossandALAE_ss > i.cap_b THEN i.cap_b ELSE a.reported_lossandALAE_ss END as capped_b_reported_lossandALAE_ss, CASE WHEN a.reported_lossandALAE_ss > i.cap_c THEN i.cap_c ELSE a.reported_lossandALAE_ss END as capped_c_reported_lossandALAE_ss, CASE WHEN a.reported_lossandALAE_ss > i.cap_d THEN i.cap_d ELSE a.reported_lossandALAE_ss END as capped_d_reported_lossandALAE_ss, CASE WHEN a.reported_lossandALAE_ss > i.cap_e THEN i.cap_e ELSE a.reported_lossandALAE_ss END as capped_e_reported_lossandALAE_ss, CASE WHEN a.reported_lossandALAE_ss > i.cap_f THEN i.cap_f ELSE a.reported_lossandALAE_ss END as capped_f_reported_lossandALAE_ss, CASE WHEN a.reported_lossandALAE_ss > i.cap_g THEN i.cap_g ELSE a.reported_lossandALAE_ss END as capped_g_reported_lossandALAE_ss,
                    CASE WHEN a.paid_lossandALAE_ss > i.cap_a THEN i.cap_a ELSE a.paid_lossandALAE_ss END as capped_a_paid_lossandALAE_ss, CASE WHEN a.paid_lossandALAE_ss > i.cap_b THEN i.cap_b ELSE a.paid_lossandALAE_ss END as capped_b_paid_lossandALAE_ss, CASE WHEN a.paid_lossandALAE_ss > i.cap_c THEN i.cap_c ELSE a.paid_lossandALAE_ss END as capped_c_paid_lossandALAE_ss, CASE WHEN a.paid_lossandALAE_ss > i.cap_d THEN i.cap_d ELSE a.paid_lossandALAE_ss END as capped_d_paid_lossandALAE_ss, CASE WHEN a.paid_lossandALAE_ss > i.cap_e THEN i.cap_e ELSE a.paid_lossandALAE_ss END as capped_e_paid_lossandALAE_ss, CASE WHEN a.paid_lossandALAE_ss > i.cap_f THEN i.cap_f ELSE a.paid_lossandALAE_ss END as capped_f_paid_lossandALAE_ss, CASE WHEN a.paid_lossandALAE_ss > i.cap_g THEN i.cap_g ELSE a.paid_lossandALAE_ss END as capped_g_paid_lossandALAE_ss,
                    CASE WHEN a.reported_loss_ss > i.cap_a THEN i.cap_a ELSE a.reported_loss_ss END as capped_a_reported_loss_ss, CASE WHEN a.reported_loss_ss > i.cap_b THEN i.cap_b ELSE a.reported_loss_ss END as capped_b_reported_loss_ss, CASE WHEN a.reported_loss_ss > i.cap_c THEN i.cap_c ELSE a.reported_loss_ss END as capped_c_reported_loss_ss, CASE WHEN a.reported_loss_ss > i.cap_d THEN i.cap_d ELSE a.reported_loss_ss END as capped_d_reported_loss_ss, CASE WHEN a.reported_loss_ss > i.cap_e THEN i.cap_e ELSE a.reported_loss_ss END as capped_e_reported_loss_ss, CASE WHEN a.reported_loss_ss > i.cap_f THEN i.cap_f ELSE a.reported_loss_ss END as capped_f_reported_loss_ss, CASE WHEN a.reported_loss_ss > i.cap_g THEN i.cap_g ELSE a.reported_loss_ss END as capped_g_reported_loss_ss,
                    CASE WHEN a.paid_loss_ss > i.cap_a THEN i.cap_a ELSE a.paid_loss_ss END as capped_a_paid_loss_ss, CASE WHEN a.paid_loss_ss > i.cap_b THEN i.cap_b ELSE a.paid_loss_ss END as capped_b_paid_loss_ss, CASE WHEN a.paid_loss_ss > i.cap_c THEN i.cap_c ELSE a.paid_loss_ss END as capped_c_paid_loss_ss, CASE WHEN a.paid_loss_ss > i.cap_d THEN i.cap_d ELSE a.paid_loss_ss END as capped_d_paid_loss_ss, CASE WHEN a.paid_loss_ss > i.cap_e THEN i.cap_e ELSE a.paid_loss_ss END as capped_e_paid_loss_ss, CASE WHEN a.paid_loss_ss > i.cap_f THEN i.cap_f ELSE a.paid_loss_ss END as capped_f_paid_loss_ss, CASE WHEN a.paid_loss_ss > i.cap_g THEN i.cap_g ELSE a.paid_loss_ss END as capped_g_paid_loss_ss,
                    coalesce((CASE WHEN a.reported_lossandALAE_ss > i.cap_a THEN i.cap_a ELSE a.reported_lossandALAE_ss END),0) - coalesce((CASE WHEN a.paid_lossandALAE_ss > i.cap_a THEN i.cap_a ELSE a.paid_lossandALAE_ss END),0) as capped_a_case_lossandALAE, coalesce((CASE WHEN a.reported_lossandALAE_ss > i.cap_b THEN i.cap_b ELSE a.reported_lossandALAE_ss END),0) - coalesce((CASE WHEN a.paid_lossandALAE_ss > i.cap_b THEN i.cap_b ELSE a.paid_lossandALAE_ss END),0) as capped_b_case_lossandALAE, coalesce((CASE WHEN a.reported_lossandALAE_ss > i.cap_c THEN i.cap_c ELSE a.reported_lossandALAE_ss END),0) - coalesce((CASE WHEN a.paid_lossandALAE_ss > i.cap_c THEN i.cap_c ELSE a.paid_lossandALAE_ss END),0) as capped_c_case_lossandALAE, coalesce((CASE WHEN a.reported_lossandALAE_ss > i.cap_d THEN i.cap_d ELSE a.reported_lossandALAE_ss END),0) - coalesce((CASE WHEN a.paid_lossandALAE_ss > i.cap_d THEN i.cap_d ELSE a.paid_lossandALAE_ss END),0) as capped_d_case_lossandALAE, coalesce((CASE WHEN a.reported_lossandALAE_ss > i.cap_e THEN i.cap_e ELSE a.reported_lossandALAE_ss END),0) - coalesce((CASE WHEN a.paid_lossandALAE_ss > i.cap_e THEN i.cap_e ELSE a.paid_lossandALAE_ss END),0) as capped_e_case_lossandALAE, coalesce((CASE WHEN a.reported_lossandALAE_ss > i.cap_f THEN i.cap_f ELSE a.reported_lossandALAE_ss END),0) - coalesce((CASE WHEN a.paid_lossandALAE_ss > i.cap_f THEN i.cap_f ELSE a.paid_lossandALAE_ss END),0) as capped_f_case_lossandALAE, coalesce((CASE WHEN a.reported_lossandALAE_ss > i.cap_g THEN i.cap_g ELSE a.reported_lossandALAE_ss END),0) - coalesce((CASE WHEN a.paid_lossandALAE_ss > i.cap_g THEN i.cap_g ELSE a.paid_lossandALAE_ss END),0) as capped_g_case_lossandALAE,
                    coalesce((CASE WHEN a.reported_loss_ss > i.cap_a THEN i.cap_a ELSE a.reported_loss_ss END),0) - coalesce((CASE WHEN a.paid_loss_ss > i.cap_a THEN i.cap_a ELSE a.paid_loss_ss END),0) as capped_a_case_loss, coalesce((CASE WHEN a.reported_loss_ss > i.cap_b THEN i.cap_b ELSE a.reported_loss_ss END),0) - coalesce((CASE WHEN a.paid_loss_ss > i.cap_b THEN i.cap_b ELSE a.paid_loss_ss END),0) as capped_b_case_loss, coalesce((CASE WHEN a.reported_loss_ss > i.cap_c THEN i.cap_c ELSE a.reported_loss_ss END),0) - coalesce((CASE WHEN a.paid_loss_ss > i.cap_c THEN i.cap_c ELSE a.paid_loss_ss END),0) as capped_c_case_loss, coalesce((CASE WHEN a.reported_loss_ss > i.cap_d THEN i.cap_d ELSE a.reported_loss_ss END),0) - coalesce((CASE WHEN a.paid_loss_ss > i.cap_d THEN i.cap_d ELSE a.paid_loss_ss END),0) as capped_d_case_loss, coalesce((CASE WHEN a.reported_loss_ss > i.cap_e THEN i.cap_e ELSE a.reported_loss_ss END),0) - coalesce((CASE WHEN a.paid_loss_ss > i.cap_e THEN i.cap_e ELSE a.paid_loss_ss END),0) as capped_e_case_loss, coalesce((CASE WHEN a.reported_loss_ss > i.cap_f THEN i.cap_f ELSE a.reported_loss_ss END),0) - coalesce((CASE WHEN a.paid_loss_ss > i.cap_f THEN i.cap_f ELSE a.paid_loss_ss END),0) as capped_f_case_loss, coalesce((CASE WHEN a.reported_loss_ss > i.cap_g THEN i.cap_g ELSE a.reported_loss_ss END),0) - coalesce((CASE WHEN a.paid_loss_ss > i.cap_g THEN i.cap_g ELSE a.paid_loss_ss END),0) as capped_g_case_loss,
                    CASE WHEN a.case_lossandALAE > 0 THEN 1 ELSE 0 END as open_CC
                FROM Claim_Level_Data as a
                CROSS JOIN Inputs i
            ),
            Claim_Manipulation as (
                SELECT
                    a.*,
                    CASE WHEN a.attorney_rep_flag = 1 THEN 'Yes' ELSE 'No' END as attorney_represented,
                    CASE WHEN a.in_suit_flag = 1 THEN 'Yes' ELSE 'No' END as litigated,
                    CASE WHEN a.lob = 'WC' and coalesce(a.reported_indemnity_loss,0) > 0 THEN 'Indemnity' WHEN a.lob = 'WC' and coalesce(a.reported_medical_loss,0) > 0 THEN 'Medical Only' WHEN a.lob = 'WC' THEN 'Expense Only' else null END as Indemnity_Medicalonly,
                    CASE WHEN a.lob = 'WC' AND a.cob_name IN ('Welding, Cutting and Metal Frame Erection', 'Secretaries and Administrative Assistants, Except Legal, Medical, and Executive', 'Masonry Work', 'Tree Services', 'Pool Halls', 'Pedicabs', 'Septic Tank System Installation, Service and Repair', 'Building Supplies', 'Museum Technicians and Conservators', 'Payroll Services', '3D Printing', 'Homeless Shelters', 'Database Administrators', 'Atmospheric and Space Scientists', 'Correspondence Clerks', 'Materials Scientists', 'Shipping, Receiving, and Traffic Clerks', 'Dispatchers, Except Police, Fire, and Ambulance', 'Medical Transcriptionists', 'Computer, Automated Teller, and Office Machine Repairers', 'Brokerage Clerks', 'Fingerprinting', 'Anthropologists and Archeologists', 'Donation Center', 'Data Entry Keyers', 'High School Teachers', 'Rehabilitation Center', 'Customs Brokers', 'Court Reporters', 'Venues and Event Spaces', 'Elder Care Planning', 'Pressure Washing', 'Halfway Houses', 'Battery Store', 'First Line Supervisors of Personal Service Workers', 'Geographers', 'Wind Turbine Service Technicians', 'Cashiers', 'Transportation, Storage, and Distribution Managers', 'Fitness and Exercise Equipment Store', 'Recreation Workers', 'Mailbox Centers', 'Hair Loss Centers', 'Beverage Store', 'Geoscientists, Except Hydrologists and Geographers', 'Inspectors, Testers, Sorters, Samplers, and Weighers', 'Dance Clubs', 'Virus Removal Services', 'Statisticians', 'Cultural Center', 'Wills, Trusts, and Probates', 'Elementary School Teachers', 'Chief Executives', 'Art Restoration', 'Information Security Analysts', 'Packing Services', 'Kids Activities', 'Process Servers', 'Animal Shelters', 'Siding Installation, Service and Repair', 'Septic Tank System Cleaning') THEN 'Not in appetite' WHEN a.lob = 'WC' THEN 'In appetite' ELSE null END as current_appetite,
                    1 as reported_CC,
                    CASE WHEN a.reported_loss > 0 THEN a.reported_loss_ss ELSE 0 END as nonzero_reported_loss_ss,
                    CASE WHEN a.reported_loss > 0 THEN a.reported_lossandalae_ss ELSE 0 END as nonzero_reported_lossandalae_ss,
                    CASE WHEN reported_loss > 0 then 1 else 0 end as nonzero_reported_CC,
                    case when reported_loss = 0 and reported_alae > 0 then 1 else 0 end as expenseonly_cc,
                    CASE WHEN open_CC = 0 then 1 else 0 END as closed_CC,
                    CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end as cwp_CC,
                    CASE WHEN open_CC = 0 and paid_loss = 0 then 1 else 0 end as cnp_CC,
                    CASE WHEN open_CC = 0 and paid_loss = 0 and paid_alae > 0 then 1 else 0 end as closed_expenseonly_CC,
                    CASE WHEN open_CC = 0 and paid_loss = 0 and paid_alae = 0 then 1 else 0 end as closed_nolossorexpense_CC,
                    CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then paid_lossandalae_ss else 0 end as paid_lossalaess_on_closed,
                    CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then capped_a_paid_lossandALAE_ss else 0 end as paid_lossalaess_on_closed_capped_a, CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then capped_b_paid_lossandALAE_ss else 0 end as paid_lossalaess_on_closed_capped_b, CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then capped_c_paid_lossandALAE_ss else 0 end as paid_lossalaess_on_closed_capped_c, CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then capped_d_paid_lossandALAE_ss else 0 end as paid_lossalaess_on_closed_capped_d, CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then capped_e_paid_lossandALAE_ss else 0 end as paid_lossalaess_on_closed_capped_e, CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then capped_f_paid_lossandALAE_ss else 0 end as paid_lossalaess_on_closed_capped_f, CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then capped_g_paid_lossandALAE_ss else 0 end as paid_lossalaess_on_closed_capped_g,
                    CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then paid_loss_ss else 0 end as paid_lossss_on_closed,
                    CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then capped_a_paid_loss_ss else 0 end as paid_lossss_on_closed_capped_a, CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then capped_b_paid_loss_ss else 0 end as paid_lossss_on_closed_capped_b, CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then capped_c_paid_loss_ss else 0 end as paid_lossss_on_closed_capped_c, CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then capped_d_paid_loss_ss else 0 end as paid_lossss_on_closed_capped_d, CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then capped_e_paid_loss_ss else 0 end as paid_lossss_on_closed_capped_e, CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then capped_f_paid_loss_ss else 0 end as paid_lossss_on_closed_capped_f, CASE WHEN (CASE WHEN open_CC = 0 and paid_loss > 0 then 1 else 0 end) = 1 then capped_g_paid_loss_ss else 0 end as paid_lossss_on_closed_capped_g,
                    CASE WHEN open_CC = 1 then reported_lossandALAE_ss else 0 end as reported_lossalaessss_on_open,
                    CASE WHEN open_CC = 1 then capped_a_reported_lossandALAE_ss else 0 end as reported_lossalaess_on_open_capped_a, CASE WHEN open_CC = 1 then capped_b_reported_lossandALAE_ss else 0 end as reported_lossalaess_on_open_capped_b, CASE WHEN open_CC = 1 then capped_c_reported_lossandALAE_ss else 0 end as reported_lossalaess_on_open_capped_c, CASE WHEN open_CC = 1 then capped_d_reported_lossandALAE_ss else 0 end as reported_lossalaess_on_open_capped_d, CASE WHEN open_CC = 1 then capped_e_reported_lossandALAE_ss else 0 end as reported_lossalaess_on_open_capped_e, CASE WHEN open_CC = 1 then capped_f_reported_lossandALAE_ss else 0 end as reported_lossalaess_on_open_capped_f, CASE WHEN open_CC = 1 then capped_g_reported_lossandALAE_ss else 0 end as reported_lossalaess_on_open_capped_g,
                    CASE WHEN open_CC = 1 then case_lossandALAE else 0 end as case_lossalae_on_open,
                    CASE WHEN open_CC = 1 then capped_a_case_lossandALAE else 0 end as case_lossalae_on_open_capped_a, CASE WHEN open_CC = 1 then capped_b_case_lossandALAE else 0 end as case_lossalae_on_open_capped_b, CASE WHEN open_CC = 1 then capped_c_case_lossandALAE else 0 end as case_lossalae_on_open_capped_c, CASE WHEN open_CC = 1 then capped_d_case_lossandALAE else 0 end as case_lossalae_on_open_capped_d, CASE WHEN open_CC = 1 then capped_e_case_lossandALAE else 0 end as case_lossalae_on_open_capped_e, CASE WHEN open_CC = 1 then capped_f_case_lossandALAE else 0 end as case_lossalae_on_open_capped_f, CASE WHEN open_CC = 1 then capped_g_case_lossandALAE else 0 end as case_lossalae_on_open_capped_g,
                    CASE WHEN open_CC = 1 then paid_lossandALAE_ss else 0 end as paid_lossalaess_on_open,
                    CASE WHEN open_CC = 1 then capped_a_paid_lossandALAE_ss else 0 end as paid_lossalaess_on_open_capped_a, CASE WHEN open_CC = 1 then capped_b_paid_lossandALAE_ss else 0 end as paid_lossalaess_on_open_capped_b, CASE WHEN open_CC = 1 then capped_c_paid_lossandALAE_ss else 0 end as paid_lossalaess_on_open_capped_c, CASE WHEN open_CC = 1 then capped_d_paid_lossandALAE_ss else 0 end as paid_lossalaess_on_open_capped_d, CASE WHEN open_CC = 1 then capped_e_paid_lossandALAE_ss else 0 end as paid_lossalaess_on_open_capped_e, CASE WHEN open_CC = 1 then capped_f_paid_lossandALAE_ss else 0 end as paid_lossalaess_on_open_capped_f, CASE WHEN open_CC = 1 then capped_g_paid_lossandALAE_ss else 0 end as paid_lossalaess_on_open_capped_g,
                    CASE WHEN open_CC = 1 then reported_loss_ss else 0 end as reported_lossss_on_open,
                    CASE WHEN open_CC = 1 then capped_a_reported_loss_ss else 0 end as reported_lossss_on_open_capped_a, CASE WHEN open_CC = 1 then capped_b_reported_loss_ss else 0 end as reported_lossss_on_open_capped_b, CASE WHEN open_CC = 1 then capped_c_reported_loss_ss else 0 end as reported_lossss_on_open_capped_c, CASE WHEN open_CC = 1 then capped_d_reported_loss_ss else 0 end as reported_lossss_on_open_capped_d, CASE WHEN open_CC = 1 then capped_e_reported_loss_ss else 0 end as reported_lossss_on_open_capped_e, CASE WHEN open_CC = 1 then capped_f_reported_loss_ss else 0 end as reported_lossss_on_open_capped_f, CASE WHEN open_CC = 1 then capped_g_reported_loss_ss else 0 end as reported_lossss_on_open_capped_g,
                    CASE WHEN open_CC = 1 then case_loss else 0 end as case_loss_on_open,
                    CASE WHEN open_CC = 1 then capped_a_case_loss else 0 end as case_loss_on_open_capped_a, CASE WHEN open_CC = 1 then capped_b_case_loss else 0 end as case_loss_on_open_capped_b, CASE WHEN open_CC = 1 then capped_c_case_loss else 0 end as case_loss_on_open_capped_c, CASE WHEN open_CC = 1 then capped_d_case_loss else 0 end as case_loss_on_open_capped_d, CASE WHEN open_CC = 1 then capped_e_case_loss else 0 end as case_loss_on_open_capped_e, CASE WHEN open_CC = 1 then capped_f_case_loss else 0 end as case_loss_on_open_capped_f, CASE WHEN open_CC = 1 then capped_g_case_loss else 0 end as case_loss_on_open_capped_g,
                    CASE WHEN open_CC = 1 then paid_loss_ss else 0 end as paid_lossss_on_open,
                    CASE WHEN open_CC = 1 then capped_a_paid_loss_ss else 0 end as paid_lossss_on_open_capped_a, CASE WHEN open_CC = 1 then capped_b_paid_loss_ss else 0 end as paid_lossss_on_open_capped_b, CASE WHEN open_CC = 1 then capped_c_paid_loss_ss else 0 end as paid_lossss_on_open_capped_c, CASE WHEN open_CC = 1 then capped_d_paid_loss_ss else 0 end as paid_lossss_on_open_capped_d, CASE WHEN open_CC = 1 then capped_e_paid_loss_ss else 0 end as paid_lossss_on_open_capped_e, CASE WHEN open_CC = 1 then capped_f_paid_loss_ss else 0 end as paid_lossss_on_open_capped_f, CASE WHEN open_CC = 1 then capped_g_paid_loss_ss else 0 end as paid_lossss_on_open_capped_g,
                    CASE WHEN a.in_suit_flag = 1 AND a.reported_loss > 0 THEN 1 ELSE 0 END as litigated_nonzero_cc,
                    CASE WHEN a.attorney_rep_flag = 1 AND a.reported_loss > 0 THEN 1 ELSE 0 END as attorney_nonzero_cc,
                    CASE WHEN a.reported_lossandALAE_ss > cap_a then 1 else 0 end as LL_a, CASE WHEN a.reported_lossandALAE_ss > cap_b then 1 else 0 end as LL_b, CASE WHEN a.reported_lossandALAE_ss > cap_c then 1 else 0 end as LL_c, CASE WHEN a.reported_lossandALAE_ss > cap_d then 1 else 0 end as LL_d, CASE WHEN a.reported_lossandALAE_ss > cap_e then 1 else 0 end as LL_e, CASE WHEN a.reported_lossandALAE_ss > cap_f then 1 else 0 end as LL_f, CASE WHEN a.reported_lossandALAE_ss > cap_g then 1 else 0 end as LL_g
                FROM Claim_Manipulation_Prep as a
            )
        SELECT
            claim_number, transaction_month, lob, cob_name, cob_group, cob_grouping, channel, state, coverage, AY, PY, RY, ay_age, py_age, ry_age, am_age, loss_cause_type_name, paygo_indicator,
            Indemnity_Medicalonly, current_appetite, attorney_represented, litigated,
            food_and_bev_express_ind, agency_type, renewal_tag,
            bop_policy_flag, -- *** ADDED NEW COLUMN ***
            NULL::double precision as earned_premium,
            NULL::double precision as earned_policy_years,
            SUM(cwp_CC) as cwp_CC, SUM(cnp_CC) as cnp_CC, SUM(closed_CC) as closed_CC, SUM(open_CC) as open_CC, SUM(reported_CC) as reported_CC, SUM(nonzero_reported_CC) as nonzero_reported_CC, SUM(expenseonly_cc) as expenseonly_cc, SUM(closed_expenseonly_CC) as closed_expenseonly_CC, SUM(closed_nolossorexpense_CC) as closed_nolossorexpense_CC,
            SUM(in_suit_flag) as litigated_claim_count, SUM(attorney_rep_flag) as attorney_rep_claim_count,
            SUM(litigated_nonzero_cc) as litigated_nonzero_claim_count,
            SUM(attorney_nonzero_cc) as attorney_nonzero_claim_count,
            SUM(paid_alae) as paid_alae, SUM(reported_alae) as reported_alae, SUM(paid_loss_ss) as paid_loss_ss, SUM(reported_loss_ss) as reported_loss_ss,
            SUM(reported_lossandALAE_ss) as reported_lossandALAE_ss, SUM(paid_lossandALAE_ss) as paid_lossandALAE_ss, SUM(case_lossandALAE) as case_lossandALAE,
            SUM(nonzero_reported_loss_ss) as nonzero_reported_loss_ss,
            SUM(nonzero_reported_lossandalae_ss) as nonzero_reported_lossandalae_ss,
            SUM(total_ss) as total_ss,
            SUM(total_incurred_xss) as total_incurred_xss,
            SUM(reported_loss) as reported_loss, -- Aggregating the new field

            SUM(paid_lossalaess_on_closed) as paid_lossalaess_on_closed, SUM(reported_lossalaessss_on_open) as reported_lossalaessss_on_open, SUM(paid_lossalaess_on_open) as paid_lossalaess_on_open,
            SUM(reported_lossss_on_open) as reported_lossss_on_open, SUM(paid_lossss_on_closed) as paid_lossss_on_closed,
            SUM(LL_a) as LL_a_CC, SUM(LL_b) as LL_b_CC, SUM(LL_c) as LL_c_CC, SUM(LL_d) as LL_d_CC, SUM(LL_e) as LL_e_CC, SUM(LL_f) as LL_f_CC, SUM(LL_g) as LL_g_CC,
            SUM(case_loss) as case_loss,
            SUM(case_lossalae_on_open) as case_lossalae_on_open,
            SUM(case_loss_on_open) as case_loss_on_open,
            SUM(paid_lossss_on_open) as paid_lossss_on_open,
            SUM(paid_lossalaess_on_closed_capped_a) as paid_lossalaess_on_closed_capped_a, SUM(paid_lossalaess_on_closed_capped_b) as paid_lossalaess_on_closed_capped_b, SUM(paid_lossalaess_on_closed_capped_c) as paid_lossalaess_on_closed_capped_c, SUM(paid_lossalaess_on_closed_capped_d) as paid_lossalaess_on_closed_capped_d, SUM(paid_lossalaess_on_closed_capped_e) as paid_lossalaess_on_closed_capped_e, SUM(paid_lossalaess_on_closed_capped_f) as paid_lossalaess_on_closed_capped_f, SUM(paid_lossalaess_on_closed_capped_g) as paid_lossalaess_on_closed_capped_g,
            SUM(paid_lossss_on_closed_capped_a) as paid_lossss_on_closed_capped_a, SUM(paid_lossss_on_closed_capped_b) as paid_lossss_on_closed_capped_b, SUM(paid_lossss_on_closed_capped_c) as paid_lossss_on_closed_capped_c, SUM(paid_lossss_on_closed_capped_d) as paid_lossss_on_closed_capped_d, SUM(paid_lossss_on_closed_capped_e) as paid_lossss_on_closed_capped_e, SUM(paid_lossss_on_closed_capped_f) as paid_lossss_on_closed_capped_f, SUM(paid_lossss_on_closed_capped_g) as paid_lossss_on_closed_capped_g,
            SUM(reported_lossalaess_on_open_capped_a) as reported_lossalaess_on_open_capped_a, SUM(reported_lossalaess_on_open_capped_b) as reported_lossalaess_on_open_capped_b, SUM(reported_lossalaess_on_open_capped_c) as reported_lossalaess_on_open_capped_c, SUM(reported_lossalaess_on_open_capped_d) as reported_lossalaess_on_open_capped_d, SUM(reported_lossalaess_on_open_capped_e) as reported_lossalaess_on_open_capped_e, SUM(reported_lossalaess_on_open_capped_f) as reported_lossalaess_on_open_capped_f, SUM(reported_lossalaess_on_open_capped_g) as reported_lossalaess_on_open_capped_g,
            SUM(case_lossalae_on_open_capped_a) as case_lossalae_on_open_capped_a, SUM(case_lossalae_on_open_capped_b) as case_lossalae_on_open_capped_b, SUM(case_lossalae_on_open_capped_c) as case_lossalae_on_open_capped_c, SUM(case_lossalae_on_open_capped_d) as case_lossalae_on_open_capped_d, SUM(case_lossalae_on_open_capped_e) as case_lossalae_on_open_capped_e, SUM(case_lossalae_on_open_capped_f) as case_lossalae_on_open_capped_f, SUM(case_lossalae_on_open_capped_g) as case_lossalae_on_open_capped_g,
            SUM(paid_lossalaess_on_open_capped_a) as paid_lossalaess_on_open_capped_a, SUM(paid_lossalaess_on_open_capped_b) as paid_lossalaess_on_open_capped_b, SUM(paid_lossalaess_on_open_capped_c) as paid_lossalaess_on_open_capped_c, SUM(paid_lossalaess_on_open_capped_d) as paid_lossalaess_on_open_capped_d, SUM(paid_lossalaess_on_open_capped_e) as paid_lossalaess_on_open_capped_e, SUM(paid_lossalaess_on_open_capped_f) as paid_lossalaess_on_open_capped_f, SUM(paid_lossalaess_on_open_capped_g) as paid_lossalaess_on_open_capped_g,
            SUM(reported_lossss_on_open_capped_a) as reported_lossss_on_open_capped_a, SUM(reported_lossss_on_open_capped_b) as reported_lossss_on_open_capped_b, SUM(reported_lossss_on_open_capped_c) as reported_lossss_on_open_capped_c, SUM(reported_lossss_on_open_capped_d) as reported_lossss_on_open_capped_d, SUM(reported_lossss_on_open_capped_e) as reported_lossss_on_open_capped_e, SUM(reported_lossss_on_open_capped_f) as reported_lossss_on_open_capped_f, SUM(reported_lossss_on_open_capped_g) as reported_lossss_on_open_capped_g,
            SUM(case_loss_on_open_capped_a) as case_loss_on_open_capped_a, SUM(case_loss_on_open_capped_b) as case_loss_on_open_capped_b, SUM(case_loss_on_open_capped_c) as case_loss_on_open_capped_c, SUM(case_loss_on_open_capped_d) as case_loss_on_open_capped_d, SUM(case_loss_on_open_capped_e) as case_loss_on_open_capped_e, SUM(case_loss_on_open_capped_f) as case_loss_on_open_capped_f, SUM(case_loss_on_open_capped_g) as case_loss_on_open_capped_g,
            SUM(paid_lossss_on_open_capped_a) as paid_lossss_on_open_capped_a, SUM(paid_lossss_on_open_capped_b) as paid_lossss_on_open_capped_b, SUM(paid_lossss_on_open_capped_c) as paid_lossss_on_open_capped_c, SUM(paid_lossss_on_open_capped_d) as paid_lossss_on_open_capped_d, SUM(paid_lossss_on_open_capped_e) as paid_lossss_on_open_capped_e, SUM(paid_lossss_on_open_capped_f) as paid_lossss_on_open_capped_f, SUM(paid_lossss_on_open_capped_g) as paid_lossss_on_open_capped_g
        FROM Claim_Manipulation
        -- *** UPDATED GROUP BY (added 26) ***
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
    ),

    Premium_Data AS (
        WITH
            Policy_Dimensions AS (
                SELECT
                    lrd.policy_reference,
                    lrd.policy_start_date,
                    lrd.policy_end_date, -- This field must exist in the ultimate_lrd table.
                    lrd.transaction_month::date as transaction_month,
                    lrd.lob, -- *** REVERTED TO ORIGINAL ***
                    lrd.cob_name,
                    lrd.cob_group,
                    case
                        when lrd.cob_group in ('Food & beverage', 'Food & beverage - deprecated') then 'Food & Beverage'
                        when lrd.cob_group in ('Retail', 'Retail - deprecated') then 'Retail'
                        when lrd.cob_group in ('Professional Services', 'Professional Services - deprecated') then 'Professional Services'
                        when lrd.cob_group = 'Day Care' then 'Day Care'
                        when lrd.cob_name in ('Tree Services', 'Welding, Cutting and Metal Frame Erection') then 'Construction'
                        when lrd.cob_group = 'Construction' then 'Construction'
                        else 'All Other'
                    end as cob_grouping,
                    case
                        when a.agency_type IN ('AP Intego', 'APIntego') then 'AP Intego'
                        when lrd.channel IN ('AP Intego', 'APIntego') then 'AP Intego'
                        else lrd.channel
                    end as channel,
                    lrd.state,
                    lrd.coverage,
                    lrd.paygo_indicator,
                    extract(year from lrd.transaction_month) as AY,
                    extract(year from lrd.policy_start_date) as PY,

                    -- START OF FINAL MODIFICATION
                    -- This maps the Premium's AY/ay_age values into the RY/ry_age columns
                    -- This will make "AY Premiums" show up on the "RY" view in Tableau
                    extract(year from lrd.transaction_month) as RY, -- RY is set to AY (transaction_month year)

                    extract(month from lrd.transaction_month) as ay_age,
                    lrd.py_age,

                    extract(month from lrd.transaction_month) as ry_age, -- ry_age is set to ay_age (transaction_month month)
                    -- END OF FINAL MODIFICATION

                    extract(month from lrd.transaction_month) as am_age,
                    lrd.food_and_bev_express_ind,
                    a.agency_type,
                    lrd.renewal_tag,
                    -- *** NEW COLUMN ADDED ***
                    CASE WHEN lrd.is_bop_policy = 1 THEN 'BOP Policy' ELSE 'Not BOP Policy' END as bop_policy_flag,
                    lrd.earned_premium
                FROM db_data_science.ultimate_lrd as lrd
                LEFT JOIN (SELECT policy_reference, agency_type FROM (SELECT policy_reference, agency_type, transaction_month, ROW_NUMBER() OVER (PARTITION BY policy_reference ORDER BY transaction_month desc, agency_type desc) AS rn FROM db_data_science.ultimate_lrd) AS agencytable_ordered_by_date WHERE rn = 1) as a ON a.policy_reference = lrd.policy_reference
                -- *** REVERTED TO ORIGINAL ***
                WHERE lrd.lob IN ('IM', 'CA', 'WC', 'PL', 'CP', 'GL')
            ),
            Policy_Exposure_Cumulative AS (
                SELECT
                    *,
                    -- Calculate the cumulative earned policy years from the start of the policy to the end of the transaction month.
                    -- This is capped by the actual policy term length.
                    CASE
                        WHEN DATEDIFF(DAY, policy_start_date, LAST_DAY(transaction_month)) > DATEDIFF(DAY, policy_start_date, policy_end_date)
                        THEN CAST(DATEDIFF(DAY, policy_start_date, policy_end_date) AS DECIMAL(18, 8)) / 365.0
                        ELSE CAST(DATEDIFF(DAY, policy_start_date, LAST_DAY(transaction_month)) AS DECIMAL(18, 8)) / 365.0
                    END as cumulative_earned_policy_years
                FROM Policy_Dimensions
            ),
            Policy_Exposure_Incremental AS (
                SELECT
                    *,
                    -- Calculate the monthly earned exposure by taking the difference from the previous month's cumulative value.
                    -- COALESCE handles the first month for each policy.
                    COALESCE(
                        cumulative_earned_policy_years - LAG(cumulative_earned_policy_years, 1) OVER (PARTITION BY policy_reference ORDER BY transaction_month),
                        cumulative_earned_policy_years
                    ) as monthly_earned_policy_years
                FROM Policy_Exposure_Cumulative
            )
        SELECT
            '' as claim_number, transaction_month, lob, cob_name, cob_group, cob_grouping, channel, state, coverage, AY, PY, RY, ay_age, py_age, ry_age, am_age,
            'N/A' as loss_cause_type_name, paygo_indicator, 'N/A' as Indemnity_Medicalonly, 'N/A' as current_appetite,
            'N/A' as attorney_represented, 'N/A' as litigated,
            food_and_bev_express_ind, agency_type, renewal_tag,
            bop_policy_flag, -- *** ADDED NEW COLUMN ***
            SUM(earned_premium) as earned_premium,
            SUM(monthly_earned_policy_years) as earned_policy_years,
            0 as cwp_CC, 0 as cnp_CC, 0 as closed_CC, 0 as open_CC, 0 as reported_CC, 0 as nonzero_reported_CC, 0 as expenseonly_cc, 0 as closed_expenseonly_CC, 0 as closed_nolossorexpense_CC,
            0 as litigated_claim_count, 0 as attorney_rep_claim_count,
            0 as litigated_nonzero_claim_count,
            0 as attorney_nonzero_claim_count,
            0 as paid_alae, 0 as reported_alae, 0 as paid_loss_ss, 0 as reported_loss_ss,
            0 as reported_lossandALAE_ss, 0 as paid_lossandALAE_ss, 0 as case_lossandALAE,
            0 as nonzero_reported_loss_ss,
            0 as nonzero_reported_lossandalae_ss,
            0 as total_ss,
            0 as total_incurred_xss,
            0 as reported_loss, -- Placeholder for the new field

            0 as paid_lossalaess_on_closed, 0 as reported_lossalaessss_on_open, 0 as paid_lossalaess_on_open,
            0 as reported_lossss_on_open, 0 as paid_lossss_on_closed,
            0 as LL_a_CC, 0 as LL_b_CC, 0 as LL_c_CC, 0 as LL_d_CC, 0 as LL_e_CC, 0 as LL_f_CC, 0 as LL_g_CC,
            0 as case_loss,
            0 as case_lossalae_on_open,
            0 as case_loss_on_open,
            0 as paid_lossss_on_open,
            0 as paid_lossalaess_on_closed_capped_a, 0 as paid_lossalaess_on_closed_capped_b, 0 as paid_lossalaess_on_closed_capped_c, 0 as paid_lossalaess_on_closed_capped_d, 0 as paid_lossalaess_on_closed_capped_e, 0 as paid_lossalaess_on_closed_capped_f, 0 as paid_lossalaess_on_closed_capped_g,
            0 as paid_lossss_on_closed_capped_a, 0 as paid_lossss_on_closed_capped_b, 0 as paid_lossss_on_closed_capped_c, 0 as paid_lossss_on_closed_capped_d, 0 as paid_lossss_on_closed_capped_e, 0 as paid_lossss_on_closed_capped_f, 0 as paid_lossss_on_closed_capped_g,
            0 as reported_lossalaess_on_open_capped_a, 0 as reported_lossalaess_on_open_capped_b, 0 as reported_lossalaess_on_open_capped_c, 0 as reported_lossalaess_on_open_capped_d, 0 as reported_lossalaess_on_open_capped_e, 0 as reported_lossalaess_on_open_capped_f, 0 as reported_lossalaess_on_open_capped_g,
            0 as case_lossalae_on_open_capped_a, 0 as case_lossalae_on_open_capped_b, 0 as case_lossalae_on_open_capped_c, 0 as case_lossalae_on_open_capped_d, 0 as case_lossalae_on_open_capped_e, 0 as case_lossalae_on_open_capped_f, 0 as case_lossalae_on_open_capped_g,
            0 as paid_lossalaess_on_open_capped_a, 0 as paid_lossalaess_on_open_capped_b, 0 as paid_lossalaess_on_open_capped_c, 0 as paid_lossalaess_on_open_capped_d, 0 as paid_lossalaess_on_open_capped_e, 0 as paid_lossalaess_on_open_capped_f, 0 as paid_lossalaess_on_open_capped_g,
            0 as reported_lossss_on_open_capped_a, 0 as reported_lossss_on_open_capped_b, 0 as reported_lossss_on_open_capped_c, 0 as reported_lossss_on_open_capped_d, 0 as reported_lossss_on_open_capped_e, 0 as reported_lossss_on_open_capped_f, 0 as reported_lossss_on_open_capped_g,
            0 as case_loss_on_open_capped_a, 0 as case_loss_on_open_capped_b, 0 as case_loss_on_open_capped_c, 0 as case_loss_on_open_capped_d, 0 as case_loss_on_open_capped_e, 0 as case_loss_on_open_capped_f, 0 as case_loss_on_open_capped_g,
            0 as paid_lossss_on_open_capped_a, 0 as paid_lossss_on_open_capped_b, 0 as paid_lossss_on_open_capped_c, 0 as paid_lossss_on_open_capped_d, 0 as paid_lossss_on_open_capped_e, 0 as paid_lossss_on_open_capped_f, 0 as paid_lossss_on_open_capped_g
        FROM Policy_Exposure_Incremental
        -- *** UPDATED GROUP BY (added 26) ***
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
    )

--- FINAL STEP: Combine the two streams and do the final aggregation ---
SELECT
    u.claim_number, u.transaction_month, u.lob, u.cob_name, u.cob_group, u.cob_grouping, u.channel, u.state, u.coverage, u.AY, u.PY, u.RY, u.ay_age, u.py_age, u.ry_age, u.am_age,
    u.loss_cause_type_name, u.paygo_indicator, u.Indemnity_Medicalonly, u.current_appetite, u.attorney_represented, u.litigated,
    u.food_and_bev_express_ind, u.agency_type, u.renewal_tag,
    u.bop_policy_flag, -- *** ADDED NEW COLUMN ***
    SUM(u.earned_premium) as earned_premium, SUM(u.earned_policy_years) as earned_policy_years,
    SUM(u.cwp_CC) as cwp_CC, SUM(u.cnp_CC) as cnp_CC, SUM(u.closed_CC) as closed_CC, SUM(u.open_CC) as open_CC, SUM(u.reported_CC) as reported_CC, SUM(u.nonzero_reported_CC) as nonzero_reported_CC, SUM(u.expenseonly_cc) as expenseonly_cc, SUM(u.closed_expenseonly_CC) as closed_expenseonly_CC, SUM(u.closed_nolossorexpense_CC) as closed_nolossorexpense_CC,
    SUM(u.litigated_claim_count) as litigated_claim_count, SUM(u.attorney_rep_claim_count) as attorney_rep_claim_count,
    SUM(u.litigated_nonzero_claim_count) as litigated_nonzero_claim_count,
    SUM(u.attorney_nonzero_claim_count) as attorney_nonzero_claim_count,
    SUM(u.paid_alae) as paid_alae, SUM(u.reported_alae) as reported_alae, SUM(u.paid_loss_ss) as paid_loss_ss, SUM(u.reported_loss_ss) as reported_loss_ss,
    SUM(u.reported_lossandALAE_ss) as reported_lossandALAE_ss, SUM(u.paid_lossandALAE_ss) as paid_lossandALAE_ss, SUM(u.case_lossandALAE) as case_lossandALAE,
    SUM(u.nonzero_reported_loss_ss) as nonzero_reported_loss_ss,
    SUM(u.nonzero_reported_lossandalae_ss) as nonzero_reported_lossandalae_ss,
    SUM(u.total_ss) as total_ss,
    SUM(u.total_incurred_xss) as total_incurred_xss,
    SUM(u.reported_loss) as reported_loss, -- Final aggregation of the new field

    SUM(u.paid_lossalaess_on_closed) as paid_lossalaess_on_closed,
    SUM(u.reported_lossalaessss_on_open) as reported_lossalaessss_on_open,
    SUM(u.paid_lossalaess_on_open) as paid_lossalaess_on_open,
    SUM(u.reported_lossss_on_open) as reported_lossss_on_open,
    SUM(u.paid_lossss_on_closed) as paid_lossss_on_closed,
    SUM(u.LL_a_CC) as LL_a_CC, SUM(u.LL_b_CC) as LL_b_CC, SUM(u.LL_c_CC) as LL_c_CC, SUM(u.LL_d_CC) as LL_d_CC, SUM(u.LL_e_CC) as LL_e_CC, SUM(u.LL_f_CC) as LL_f_CC, SUM(u.LL_g_CC) as LL_g_CC,
    SUM(u.case_loss) as case_loss,
    SUM(u.case_lossalae_on_open) as case_lossalae_on_open,
    SUM(u.case_loss_on_open) as case_loss_on_open,
    SUM(u.paid_lossss_on_open) as paid_lossss_on_open,
    SUM(u.paid_lossalaess_on_closed_capped_a) as paid_lossalaess_on_closed_capped_a, SUM(u.paid_lossalaess_on_closed_capped_b) as paid_lossalaess_on_closed_capped_b, SUM(u.paid_lossalaess_on_closed_capped_c) as paid_lossalaess_on_closed_capped_c, SUM(u.paid_lossalaess_on_closed_capped_d) as paid_lossalaess_on_closed_capped_d, SUM(u.paid_lossalaess_on_closed_capped_e) as paid_lossalaess_on_closed_capped_e, SUM(u.paid_lossalaess_on_closed_capped_f) as paid_lossalaess_on_closed_capped_f, SUM(u.paid_lossalaess_on_closed_capped_g) as paid_lossalaess_on_closed_capped_g,
    SUM(u.paid_lossss_on_closed_capped_a) as paid_lossss_on_closed_capped_a, SUM(u.paid_lossss_on_closed_capped_b) as paid_lossss_on_closed_capped_b, SUM(u.paid_lossss_on_closed_capped_c) as paid_lossss_on_closed_capped_c, SUM(u.paid_lossss_on_closed_capped_d) as paid_lossss_on_closed_capped_d, SUM(u.paid_lossss_on_closed_capped_e) as paid_lossss_on_closed_capped_e, SUM(u.paid_lossss_on_closed_capped_f) as paid_lossss_on_closed_capped_f, SUM(u.paid_lossss_on_closed_capped_g) as paid_lossss_on_closed_capped_g,
    SUM(u.reported_lossalaess_on_open_capped_a) as reported_lossalaess_on_open_capped_a, SUM(u.reported_lossalaess_on_open_capped_b) as reported_lossalaess_on_open_capped_b, SUM(u.reported_lossalaess_on_open_capped_c) as reported_lossalaess_on_open_capped_c, SUM(u.reported_lossalaess_on_open_capped_d) as reported_lossalaess_on_open_capped_d, SUM(u.reported_lossalaess_on_open_capped_e) as reported_lossalaess_on_open_capped_e, SUM(u.reported_lossalaess_on_open_capped_f) as reported_lossalaess_on_open_capped_f, SUM(u.reported_lossalaess_on_open_capped_g) as reported_lossalaess_on_open_capped_g,
    SUM(u.case_lossalae_on_open_capped_a) as case_lossalae_on_open_capped_a, SUM(u.case_lossalae_on_open_capped_b) as case_lossalae_on_open_capped_b, SUM(u.case_lossalae_on_open_capped_c) as case_lossalae_on_open_capped_c, SUM(u.case_lossalae_on_open_capped_d) as case_lossalae_on_open_capped_d, SUM(u.case_lossalae_on_open_capped_e) as case_lossalae_on_open_capped_e, SUM(u.case_lossalae_on_open_capped_f) as case_lossalae_on_open_capped_f, SUM(u.case_lossalae_on_open_capped_g) as case_lossalae_on_open_capped_g,
    SUM(u.paid_lossalaess_on_open_capped_a) as paid_lossalaess_on_open_capped_a, SUM(u.paid_lossalaess_on_open_capped_b) as paid_lossalaess_on_open_capped_b, SUM(u.paid_lossalaess_on_open_capped_c) as paid_lossalaess_on_open_capped_c, SUM(u.paid_lossalaess_on_open_capped_d) as paid_lossalaess_on_open_capped_d, SUM(u.paid_lossalaess_on_open_capped_e) as paid_lossalaess_on_open_capped_e, SUM(u.paid_lossalaess_on_open_capped_f) as paid_lossalaess_on_open_capped_f, SUM(u.paid_lossalaess_on_open_capped_g) as paid_lossalaess_on_open_capped_g,
    SUM(u.reported_lossss_on_open_capped_a) as reported_lossss_on_open_capped_a, SUM(u.reported_lossss_on_open_capped_b) as reported_lossss_on_open_capped_b, SUM(u.reported_lossss_on_open_capped_c) as reported_lossss_on_open_capped_c, SUM(u.reported_lossss_on_open_capped_d) as reported_lossss_on_open_capped_d, SUM(u.reported_lossss_on_open_capped_e) as reported_lossss_on_open_capped_e, SUM(u.reported_lossss_on_open_capped_f) as reported_lossss_on_open_capped_f, SUM(u.reported_lossss_on_open_capped_g) as reported_lossss_on_open_capped_g,
    SUM(u.case_loss_on_open_capped_a) as case_loss_on_open_capped_a, SUM(u.case_loss_on_open_capped_b) as case_loss_on_open_capped_b, SUM(u.case_loss_on_open_capped_c) as case_loss_on_open_capped_c, SUM(u.case_loss_on_open_capped_d) as case_loss_on_open_capped_d, SUM(u.case_loss_on_open_capped_e) as case_loss_on_open_capped_e, SUM(u.case_loss_on_open_capped_f) as case_loss_on_open_capped_f, SUM(u.case_loss_on_open_capped_g) as case_loss_on_open_capped_g,
    SUM(u.paid_lossss_on_open_capped_a) as paid_lossss_on_open_capped_a, SUM(u.paid_lossss_on_open_capped_b) as paid_lossss_on_open_capped_b, SUM(u.paid_lossss_on_open_capped_c) as paid_lossss_on_open_capped_c, SUM(u.paid_lossss_on_open_capped_d) as paid_lossss_on_open_capped_d, SUM(u.paid_lossss_on_open_capped_e) as paid_lossss_on_open_capped_e, SUM(u.paid_lossss_on_open_capped_f) as paid_lossss_on_open_capped_f, SUM(u.paid_lossss_on_open_capped_g) as paid_lossss_on_open_capped_g
FROM (
    SELECT * FROM Claims_Data
    UNION ALL
    SELECT * FROM Premium_Data
) u
-- *** UPDATED GROUP BY (added 26) ***
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
-- *** UPDATED ORDER BY (added bop_policy_flag) ***
ORDER BY u.AY, u.PY, u.RY, u.ay_age, u.py_age, u.ry_age, u.lob, u.bop_policy_flag);