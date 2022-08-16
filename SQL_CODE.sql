

/****** UPDATING INTEREST RATE DATA ******/

/**** Getting Data for the Interest Rate Change Table ****/
CREATE OR REPLACE TEMPORARY TABLE int_rate_temp_step1 AS (
SELECT CAST(parentid AS VARCHAR(18)) AS loan_id
      ,TO_DATE(createddate) AS system_dt
      ,CAST(ZEROIFNULL(oldvalue) AS FLOAT) AS prev_int_rate
      ,CAST(ZEROIFNULL(newvalue) AS FLOAT) AS curr_int_rate
      ,ROW_NUMBER() OVER (PARTITION BY parentid ORDER BY createddate ASC) AS trans_number
  FROM loan_dw_prod.cloudlending.loan_account_history AS hist
 WHERE hist.field = 'loan__Interest_Rate__c'
);


/**** Query Interest Rate Changes ****/
CREATE OR REPLACE TEMPORARY TABLE int_rate_temp_step2 AS (
SELECT hist.loan_id
      ,hist.system_dt
      ,hist.prev_int_rate
      ,CASE WHEN hist.system_dt = next_hist.system_dt AND next_hist.curr_int_rate <> hist.prev_int_rate THEN next_hist.curr_int_rate
       ELSE hist.curr_int_rate
       END AS curr_int_rate
      ,CASE WHEN hist.system_dt = prev_hist.system_dt AND prev_hist.prev_int_rate = hist.curr_int_rate THEN 0 --Same day reversal
       WHEN hist.system_dt = next_hist.system_dt AND next_hist.curr_int_rate = hist.prev_int_rate THEN 0 --Same day reversal
       WHEN hist.system_dt = prev_hist.system_dt AND prev_hist.prev_int_rate <> hist.curr_int_rate THEN 0 --Same day update
       ELSE 1
       END AS reversal_filter
  FROM int_rate_temp_step1 AS hist
  LEFT JOIN int_rate_temp_step1 AS prev_hist
    ON hist.loan_id = prev_hist.loan_id
   AND (hist.trans_number - 1)  = prev_hist.trans_number
  LEFT JOIN int_rate_temp_step1 AS next_hist
    ON hist.loan_id = next_hist.loan_id
   AND (hist.trans_number + 1)  = next_hist.trans_number
);


/**** First Pass on Current and Previous Rates ****/
CREATE OR REPLACE TEMPORARY TABLE int_rate_temp_step3 AS (
SELECT loan_id
      ,system_dt AS chng_dt
      ,prev_int_rate
      ,curr_int_rate
      ,ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY system_dt ASC) AS trans_number
  FROM int_rate_temp_step2
 WHERE reversal_filter = 1
);

 
/**** Query expected payment dates for filtering ****/
/* It seems that permanent rate changes happen prior*/
/* to scheduled payment dates                       */
CREATE OR REPLACE TABLE exp_pmt_dts_temp AS (
SELECT CAST(loan_account AS VARCHAR(18)) AS loan_id
      ,TO_DATE(DATEADD(DAY, -1, loan_due_date)) AS exp_pmt_dt
  FROM loan_dw_prod.cloudlending.loan_repayment_schedule
);


/**** Adjusting Interest Rate Start Dates to Last Transaction Date ****/
/* Only for suspected permanent rate changs                           */
CREATE OR REPLACE TABLE int_rate_temp_perm_rate AS (
SELECT loan_id
      ,chng_dt
      ,DATEADD(DAY, 1, chng_dt_adj) AS chng_dt_adj
      ,curr_int_rate
      ,prev_int_rate
  FROM (SELECT loan_id
  	       ,chng_dt
	       ,curr_int_rate
	       ,prev_int_rate
	       ,MAX(loan_transaction_date) AS chng_dt_adj
	   FROM (SELECT step3.loan_id
	               ,step3.chng_dt
	               ,step3.curr_int_rate
	               ,step3.prev_int_rate
	               ,tran.loan_transaction_date
	           FROM int_rate_temp_step3 AS step3
	           LEFT JOIN mart_finance.lgilius.loan_transactions AS tran
	             ON step3.loan_id = tran.loan_id
	            AND step3.chng_dt > tran.loan_transaction_date
	            AND tran.revs_flag = 0
	          INNER JOIN exp_pmt_dts_temp AS dts
	             ON step3.loan_id = dts.loan_id
	            AND step3.chng_dt = dts.exp_pmt_dt
	                ) AS tran_match
	  GROUP BY 1, 2, 3, 4
	        ) AS dt_adj
);


/**** Collect expected temporary rate changes ****/
CREATE OR REPLACE TEMPORARY TABLE int_rate_temp_step4 AS (
SELECT step3.loan_id
      ,step3.chng_dt
      ,step3.prev_int_rate
      ,step3.curr_int_rate
      ,step3.trans_number
  FROM int_rate_temp_step3 AS step3
  LEFT JOIN int_rate_temp_perm_rate AS exp_perm
    ON step3.loan_id = exp_perm.loan_id
   AND step3.chng_dt = exp_perm.chng_dt
 WHERE exp_perm.chng_dt_adj IS NULL
);


/**** Adjust expected temporary rate change start dates ****/
CREATE OR REPLACE TEMPORARY TABLE int_rate_temp_step5 AS (
SELECT step4.loan_id
      ,CASE WHEN step4.prev_int_rate = next_chng.curr_int_rate THEN DATEADD(DAY, 1, step4.chng_dt)
       ELSE step4.chng_dt
       END AS chng_dt
      ,step4.prev_int_rate
      ,step4.curr_int_rate
      ,CASE WHEN step4.curr_int_rate = prev_chng.prev_int_rate THEN 1
       WHEN step4.prev_int_rate = next_chng.curr_int_rate THEN 1
       ELSE 0
       END AS temp_rate_chng_flag
  FROM int_rate_temp_step4 AS step4
  LEFT JOIN int_rate_temp_step4 AS prev_chng
    ON step4.loan_id = prev_chng.loan_id
   AND (step4.trans_number - 1) = prev_chng.trans_number
  LEFT JOIN int_rate_temp_step4 AS next_chng
    ON step4.loan_id = next_chng.loan_id
   AND (step4.trans_number + 1) = next_chng.trans_number
);

 
/**** Treat remaining as permanent ****/
CREATE OR REPLACE TEMPORARY TABLE int_rate_temp_step6 AS (
SELECT loan_id
      ,chng_dt
      ,DATEADD(DAY, 1, chng_dt_adj) AS chng_dt_adj
      ,curr_int_rate
      ,prev_int_rate
  FROM (SELECT loan_id
  	       ,chng_dt
	       ,curr_int_rate
	       ,prev_int_rate
	       ,MAX(loan_transaction_date) AS chng_dt_adj
	   FROM (SELECT step5.loan_id
	               ,step5.chng_dt
	               ,step5.curr_int_rate
	               ,step5.prev_int_rate
	               ,tran.loan_transaction_date
	           FROM int_rate_temp_step5 AS step5
	           LEFT JOIN mart_finance.lgilius.loan_transactions AS tran
	             ON step5.loan_id = tran.loan_id
	            AND step5.chng_dt >= tran.loan_transaction_date
	            AND tran.revs_flag = 0
	          WHERE step5.temp_rate_chng_flag = 0
	                ) AS tran_match
	  GROUP BY 1, 2, 3, 4
	        ) AS dt_adj
);


CREATE OR REPLACE TEMPORARY TABLE int_rate_temp_step7 AS (
SELECT loan_id
      ,start_dt
      ,curr_int_rate
      ,ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY start_dt ASC) AS trans_number
  FROM (SELECT loan_id
              ,chng_dt_adj AS start_dt
              ,curr_int_rate
          FROM int_rate_temp_perm_rate
 
         UNION ALL
 
        SELECT loan_id
              ,chng_dt AS start_dt
              ,curr_int_rate
          FROM int_rate_temp_step5
         WHERE temp_rate_chng_flag = 1
 
         UNION ALL

        SELECT loan_id
              ,chng_dt_adj AS start_dt
              ,curr_int_rate
          FROM int_rate_temp_step6
               ) AS int_data
);


CREATE OR REPLACE TABLE mart_finance.lgilius.int_rate_history AS (
SELECT curr.loan_id
      ,curr.start_dt
      ,CASE WHEN nxt.start_dt IS NULL THEN '9999-12-31'
       ELSE DATEADD(DAY, -1, nxt.start_dt)
       END AS end_dt
      ,curr.curr_int_rate
  FROM int_rate_temp_step7 AS curr
  LEFT JOIN int_rate_temp_step7 AS nxt
    ON curr.loan_id = nxt.loan_id
   AND (curr.trans_number + 1) = nxt.trans_number
);


/****** LOAN DETAILS ******/


/*** Build out the loan dimensions and reformat ***/
CREATE OR REPLACE TEMPORARY TABLE dim_temp AS (
SELECT CAST(loan.id AS VARCHAR(18)) AS loan_id
      ,CAST(loan.name AS VARCHAR(12)) AS loan_name
      ,CAST(contact.customer_id AS VARCHAR(12)) AS customer_id
      ,CAST(contact.ambassador_id AS VARCHAR(12)) AS ambassador_id
      ,CAST(loan.application AS VARCHAR(18)) AS app_id
      ,CAST(dim.appname AS VARCHAR(14)) AS app_name
      ,TO_DATE(loan.loan_approval_date) AS approval_dt
      ,TO_DATE(loan.loan_application_date) AS app_dt
      ,TO_DATE(loan.loan_accrual_start_date) AS loan_start_dt
      ,loan.loan_amount AS orig_loan_amt
      ,CAST(dim.bank_partner_name AS VARCHAR(25)) AS bank_partner_name
      ,CAST(COALESCE(loan.legal_entity, office.name) AS VARCHAR(25)) AS legal_entity
      ,CAST(loan.originating_state AS VARCHAR(2)) AS state
      ,CAST(loan.loan_contract_type AS VARCHAR(9)) AS loan_type
      ,UPPER( CASE WHEN loan.loan_contract_type = 'Refinance' THEN 'Uploan'
              WHEN app.advertising_method IS NULL THEN 'Uploan'
              WHEN app.type_formula = 'Refinance' THEN 'Uploan'
              WHEN app.type_formula = 'Reloan' THEN 'Reloan'
              WHEN add_mth.name = 'Refinance' THEN 'Uploan'
              WHEN app.type_formula <> 'New' THEN 'Uploan'
              ELSE 'New'
              END) AS loan_type_sub
      ,prod.originating_bank
      ,CASE WHEN prod.originating_bank IN('Capital Community Bank')
            AND loan.originating_state NOT IN('NE')
            AND TO_DATE(loan.createddate) >= '04/19/2022'
       THEN 'TRS'
       ELSE 'NON-TRS'
       END AS bs_type
      ,TO_DATE(loan.purchase_date) AS purchase_date
      ,CAST(loan.delinquency_status AS VARCHAR(11)) AS pmt_status
      ,CAST(loan.loan_interest_type AS VARCHAR(5)) AS rate_type
      ,loan.loan_contractual_interest_rate AS int_rate_orig
      ,CAST(loan.loan_repayment_period_interest_calculation AS VARCHAR(15)) AS int_calc_type
      ,CAST(loan.loan_repayment_procedure AS VARCHAR(30)) AS int_calc_procedure
      ,CAST(loan.loan_time_counting_method AS VARCHAR(15)) AS day_year_type
      ,CAST(loan.loan_frequency_of_loan_payment AS VARCHAR(15)) AS pmt_freq
      ,loan.retention_percentage
      ,CAST(app.credit_score AS FLOAT) AS app_credit_score
      ,CAST( CASE WHEN app.source_type = 'Low' AND app.credit_score >= 521 THEN 1
	      WHEN app.source_type = 'Low' AND app.credit_score >= 472 THEN 2
	      WHEN app.source_type = 'Low' AND app.credit_score >= 416 THEN 3
	      WHEN app.source_type = 'Low' AND app.credit_score >= 377 THEN 4
	      WHEN app.source_type = 'Low' AND app.credit_score >= 360 THEN 5
	      WHEN app.source_type = 'Low' AND prod.originating_bank = 'Core' AND app.credit_score >= 345 THEN 6 --for Core
	      WHEN app.source_type = 'Low' AND prod.originating_bank <> 'Core' AND app.credit_score >= 350 THEN 6 --for Bank Partners
	      WHEN app.source_type='Low' AND app.credit_score >= 330 THEN 7
	      --For Uploan/Reloan
	      WHEN app.source_type <> 'Low' AND app.credit_score >= 472 THEN 1
	      WHEN app.source_type <> 'Low' AND app.credit_score >= 397 THEN 2
	      WHEN app.source_type <> 'Low' AND app.credit_score >= 351 THEN 3
	      WHEN app.source_type = 'Reloan' AND app.credit_score >= 250 THEN 4
	      WHEN app.source_type = 'Refinance' AND app.app_status = 'LOAN APPROVED' THEN 4
	      ELSE 999
	      END AS INTEGER) AS app_risk_segment
  FROM loan_dw_prod.cloudlending.loan_account AS loan
  LEFT JOIN loan_dw_prod.public.loan_dim AS dim
    ON loan.id = dim.loanid
  LEFT JOIN loan_dw_prod.cloudlending.loan_office_name AS office
    ON loan.loan_branch = office.id
  LEFT JOIN (SELECT CAST(id AS VARCHAR(18)) AS app_id
                   ,CAST(contact AS VARCHAR(18)) AS contact
                   ,CAST(source_type AS VARCHAR(10)) AS source_type
                   ,advertising_method
                   ,type_formula
                   ,CAST(status AS VARCHAR(40)) AS app_status
                   ,CASE WHEN total_score > 0 THEN total_score
                    ELSE bcs_score
                    END AS credit_score
               FROM loan_dw_prod.cloudlending.applications
                    ) AS app
    ON app.app_id = CAST(loan.application AS VARCHAR(18))
  LEFT JOIN loan_dw_prod.cloudlending.loan_product AS prod 
    ON prod.id = loan.loan_product_name
  LEFT JOIN loan_dw_prod.cloudlending.advertising_method AS add_mth
    ON add_mth.id = app.advertising_method
  LEFT JOIN (SELECT id
                   ,contact_external_id
                   ,customer_id
                   ,ambassador_id
               FROM loan_dw_prod.cloudlending.contact
                    ) AS contact
    ON app.contact = CAST(contact.id AS VARCHAR(18))
 WHERE loan.delinquency_status <> 'CANCELED'
   AND loan.loan_accrual_start_date >= DATEADD(YEAR, -5, TO_DATE(CURRENT_TIMESTAMP()))
);


/****** QUERY TRANSACTION DATA ******/


/**** Query and Clean Payment Transaction Data ****/
CREATE OR REPLACE TEMPORARY TABLE pmt_trans_temp AS (
SELECT CAST(pmt.loan_account AS VARCHAR(18)) AS loan_id
      ,TO_DATE(pmt.loan_transaction_date) AS loan_transaction_date
      ,CASE WHEN pmt.loan_reversed = 'TRUE' THEN TO_DATE(pmt.systemmodstamp)
       ELSE NULL
       END AS loan_reversal_date
      ,CASE WHEN pmt.loan_reversed = 'TRUE' THEN 1
       ELSE 0
       END AS revs_flag
      ,CASE WHEN loan_write_off_recovery_payment = FALSE AND loan_refinance_transaction IS NULL THEN pmt.loan_principal
       END AS prin_pmt
      ,CASE WHEN loan_write_off_recovery_payment = FALSE AND loan_refinance_transaction IS NULL THEN pmt.loan_interest
       END AS int_pmt
      ,pmt.loan_balance
      ,CASE WHEN loan_write_off_recovery_payment = FALSE AND loan_refinance_transaction IS NOT NULL THEN pmt.loan_principal
       END AS prin_refi
      ,CASE WHEN loan_write_off_recovery_payment = FALSE AND loan_refinance_transaction IS NOT NULL THEN pmt.loan_interest
       END AS int_refi
      ,CASE WHEN loan_write_off_recovery_payment = TRUE THEN pmt.loan_principal
       END AS prin_recovery_pmt
      ,CASE WHEN loan_write_off_recovery_payment = TRUE THEN pmt.loan_interest
       END AS int_recovery_pmt
      ,CASE WHEN loan_write_off_recovery_payment = FALSE AND loan_refinance_transaction IS NOT NULL THEN CAST( SUBSTR(loan_cheque_number, 15, 100) AS VARCHAR(12))
       END AS refi_loan_name
  FROM loan_dw_prod.cloudlending.loan_payment_transaction AS pmt
);


/**** Query loan account history for charge off data ****/
CREATE OR REPLACE TEMPORARY TABLE co_new_data_temp AS (
SELECT CAST(parentid AS VARCHAR(18)) AS loan_id
      ,CASE WHEN field = 'loan__Charged_Off_Date__c' THEN newvalue
       ELSE CAST(NULL AS DATE)
       END AS charge_off_date
  FROM loan_dw_prod.cloudlending.loan_account_history
 WHERE charge_off_date IS NOT NULL
);


/**** Reorganize the charge off data into a usable table ****/
CREATE OR REPLACE TEMPORARY TABLE co_new_data_temp_2 AS (
SELECT co.loan_id
      ,co.charge_off_date
      ,prin.newvalue AS prin_charge_off
      ,intst.newvalue AS int_charge_off
      ,CASE WHEN bankruptcy_flag IS NULL THEN 0
       ELSE bankruptcy_flag
       END AS bankruptcy_flag
  FROM co_new_data_temp AS co
  LEFT JOIN (SELECT CAST(parentid AS VARCHAR(18)) AS loan_id
                   ,newvalue
                   ,ROW_NUMBER() OVER (PARTITION BY parentid ORDER BY createddate DESC) AS tran_number
               FROM loan_dw_prod.cloudlending.loan_account_history
              WHERE field = 'loan__Charged_Off_Principal__c'
                    ) AS prin
    ON co.loan_id = prin.loan_id
   AND prin.tran_number = 1
  LEFT JOIN (SELECT CAST(parentid AS VARCHAR(18)) AS loan_id
                   ,newvalue
                   ,ROW_NUMBER() OVER (PARTITION BY parentid ORDER BY createddate DESC) AS tran_number
               FROM loan_dw_prod.cloudlending.loan_account_history
              WHERE field = 'loan__Charged_Off_Interest__c'
                    ) AS intst
    ON co.loan_id = intst.loan_id
   AND intst.tran_number = 1
  LEFT JOIN (SELECT CAST(parentid AS VARCHAR(18)) AS loan_id
                   ,1 AS bankruptcy_flag
               FROM loan_dw_prod.cloudlending.loan_account_history
              WHERE field = 'Bankruptcy_Status__c'
                AND newvalue = 'Filed'
                    ) AS bnk
    ON co.loan_id = bnk.loan_id
);


/**** Query interest waived information ****/
CREATE OR REPLACE TEMPORARY TABLE int_waive_temp AS (
SELECT CAST(parentid AS VARCHAR(18)) AS loan_id
      ,CASE WHEN field = 'loan__Interest_Waived__c' THEN TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Chicago', createddate))
       ELSE CAST(NULL AS DATE)
       END AS interest_waived_date
      ,CASE WHEN field = 'loan__Interest_Waived__c' THEN newvalue
       ELSE NULL
       END AS int_waived
  FROM loan_dw_prod.cloudlending.loan_account_history
 WHERE interest_waived_date IS NOT NULL
);

/**** Query and Clean Other Transaction Data ****/
CREATE OR REPLACE TEMPORARY TABLE other_trans_temp_step1 AS (
SELECT CAST(other.loan_account AS VARCHAR(18)) AS loan_id
      ,TO_DATE(other.loan_txn_date) AS loan_transaction_date
      ,CASE WHEN other.loan_reversed = 'TRUE' THEN TO_DATE(other.systemmodstamp)
       ELSE NULL
       END AS loan_reversal_date
      ,CASE WHEN other.loan_reversed = 'TRUE' THEN 1
       ELSE 0
       END AS revs_flag
      ,CAST(other.loan_transaction_type AS VARCHAR(50)) AS tran_type
      ,CASE WHEN other.loan_transaction_type = 'PrincipalAdjustment-Subtract' THEN other.loan_txn_amt
       WHEN other.loan_transaction_type = 'PrincipalAdjustment-Add' THEN -1*other.loan_txn_amt 
       ELSE 0
       END AS prin_bal_adj
      ,other.loan_charged_off_principal AS prin_chrg_off
      ,other.loan_charged_off_interest AS int_chrg_off
      ,CASE WHEN other.loan_transaction_type = 'Reschedule' AND other.createddate = reschd_tran.max_dt THEN loan_total_interest_due
       ELSE 0
       END AS int_reschd_adj
      ,CASE WHEN other.loan_transaction_type = 'Reschedule' AND other.createddate = reschd_tran.max_dt THEN 1
       WHEN other.loan_transaction_type <> 'Reschedule' THEN 1
       ELSE 0
       END AS tran_filter
  FROM loan_dw_prod.cloudlending.loan_other_transaction AS other
  LEFT JOIN (SELECT CAST(loan_account AS VARCHAR(18)) AS loan_id
                   ,TO_DATE(loan_txn_date) AS loan_transaction_date
                   ,MAX(createddate) AS max_dt
               FROM loan_dw_prod.cloudlending.loan_other_transaction
              WHERE loan_transaction_type = 'Reschedule'
              GROUP BY 1, 2
                    ) AS reschd_tran
    ON other.loan_account = reschd_tran.loan_id
   AND other.loan_txn_date = reschd_tran.loan_transaction_date
 WHERE other.loan_transaction_type IN('Reschedule', 'Charge Off', 'PrincipalAdjustment-Subtract')
       -- PrincipalAdjustment-Add is left off because it seems to duplicated as a disbursement
   AND tran_filter = 1
);

 
CREATE OR REPLACE TEMPORARY TABLE other_trans_temp_step2 AS (
SELECT s1.loan_id
      ,s1.loan_transaction_date
      ,s1.loan_reversal_date
      ,s1.revs_flag
      ,s1.tran_type
      ,s1.prin_bal_adj
      ,s1.prin_chrg_off
      ,s1.int_chrg_off
      ,s1.int_reschd_adj
      ,CASE WHEN s1.tran_type NOT IN('Charge Off') THEN s1.tran_filter
       WHEN s1.loan_transaction_date = max_dt.max_dt THEN 1
       ELSE 0
       END AS tran_filter
  FROM other_trans_temp_step1 AS s1
  LEFT JOIN (SELECT loan_id
		     ,tran_type
		     ,MAX(loan_transaction_date) AS max_dt
		 FROM other_trans_temp_step1
		WHERE tran_type IN('Charge Off')
		  AND revs_flag = 0
		GROUP BY 1, 2
		      ) AS max_dt
    ON s1.loan_id = max_dt.loan_id
   AND s1.tran_type = max_dt.tran_type
);


CREATE OR REPLACE TEMPORARY TABLE other_trans_temp3 AS (
SELECT *
  FROM other_trans_temp_step2
 WHERE tran_filter = 1
);


/**** Query and Clean Reversed Other Transaction Data ****/
CREATE OR REPLACE TEMPORARY TABLE other_trans_temp AS (
SELECT loan_id
      ,loan_transaction_date
      ,loan_reversal_date
      ,revs_flag
      ,0 AS bankruptcy_flag
      ,tran_type
      ,prin_bal_adj
      ,prin_chrg_off
      ,int_chrg_off
      ,int_reschd_adj
      ,0 AS int_waived
  FROM other_trans_temp3

 UNION ALL

SELECT co_new.loan_id
      ,co_new.charge_off_date AS loan_transaction_date
      ,CAST(NULL AS DATE) AS loan_reversal_date
      ,0 AS revs_flag
      ,bankruptcy_flag
      ,'Charge Off' AS tran_type
      ,0 AS prin_bal_adj
      ,prin_charge_off AS prin_chrg_off
      ,int_charge_off AS int_chrg_off
      ,0 AS int_reschd_adj
      ,0 AS int_waived
  FROM co_new_data_temp_2 AS co_new
  LEFT JOIN (SELECT DISTINCT loan_id
               FROM other_trans_temp3
              WHERE prin_chrg_off > 0 OR int_chrg_off > 0
                    ) AS othr
    ON co_new.loan_id = othr.loan_id
 WHERE othr.loan_id IS NULL
 
 UNION ALL

SELECT loan_id
      ,interest_waived_date AS loan_transaction_date
      ,CAST(NULL AS DATE) AS loan_reversal_date
      ,0 AS revs_flag
      ,0 AS bankruptcy_flag
      ,'Interest Waived' AS tran_type
      ,0 AS prin_bal_adj
      ,0 AS prin_chrg_off
      ,0 AS int_chrg_off
      ,0 AS int_reschd_adj
      ,int_waived
  FROM int_waive_temp
);


/**** Query disbursement transactions ****/
CREATE OR REPLACE TEMPORARY TABLE disb_trans_temp AS (
SELECT CAST(loan_account AS VARCHAR(18)) AS loan_id
      ,TO_DATE(loan_disbursal_date) AS loan_transaction_date
      ,loan_disbursed_amt AS grs_disb_amt
      ,loan_financed_amount AS net_disb_amt
  FROM loan_dw_prod.cloudlending.loan_disbursal_transaction
 WHERE loan_cleared = TRUE
   AND loan_reversed = FALSE
);


CREATE OR REPLACE TEMPORARY TABLE trans_temp AS (
SELECT loan_id
      ,loan_transaction_date
      ,loan_reversal_date
      ,revs_flag
      ,0 AS bankruptcy_flag
      ,ZEROIFNULL(prin_pmt) AS prin_pmt
      ,ZEROIFNULL(int_pmt) AS int_pmt
      ,ZEROIFNULL(prin_refi) AS prin_refi
      ,ZEROIFNULL(int_refi) AS int_refi
      ,ZEROIFNULL(prin_recovery_pmt) AS prin_recovery_pmt
      ,ZEROIFNULL(int_recovery_pmt) AS int_recovery_pmt
      ,refi_loan_name
      ,0 AS prin_bal_adj
      ,0 AS prin_chrg_off
      ,0 AS int_chrg_off
      ,0 AS int_reschd_adj
      ,0 AS grs_disb_amt
      ,0 AS net_disb_amt
      ,0 AS int_waived
  FROM pmt_trans_temp
  
 UNION ALL

SELECT loan_id
      ,loan_transaction_date
      ,loan_reversal_date
      ,revs_flag
      ,bankruptcy_flag
      ,0 AS prin_pmt
      ,0 AS int_pmt
      ,0 AS prin_refi
      ,0 AS int_refi
      ,0 AS prin_recovery_pmt
      ,0 AS int_recovery_pmt
      ,CAST(NULL AS VARCHAR(12)) AS refi_loan_name
      ,ZEROIFNULL(prin_bal_adj) AS prin_bal_adj
      ,ZEROIFNULL(prin_chrg_off) AS prin_chrg_off
      ,ZEROIFNULL(int_chrg_off) AS int_chrg_off
      ,ZEROIFNULL(int_reschd_adj) AS int_reschd_adj
      ,0 AS grs_disb_amt
      ,0 AS net_disb_amt
      ,ZEROIFNULL(int_waived) AS int_waived
  FROM other_trans_temp
 
 UNION ALL
 
SELECT loan_id
      ,loan_transaction_date
      ,CAST(NULL AS DATE) AS loan_reversal_date
      ,0 AS revs_flag
      ,0 AS bankruptcy_flag
      ,0 AS prin_pmt
      ,0 AS int_pmt
      ,0 AS prin_refi
      ,0 AS int_refi
      ,0 AS prin_recovery_pmt
      ,0 AS int_recovery_pmt
      ,CAST(NULL AS VARCHAR(12)) AS refi_loan_name
      ,0 AS prin_bal_adj
      ,0 AS prin_chrg_off
      ,0 AS int_chrg_off
      ,0 AS int_reschd_adj
      ,ZEROIFNULL(grs_disb_amt) AS grs_disb_amt
      ,ZEROIFNULL(net_disb_amt) AS net_disb_amt
      ,0 AS int_waived
  FROM disb_trans_temp
);


CREATE OR REPLACE TABLE mart_finance.lgilius.loan_transactions AS (
SELECT loan_id
      --,refi_loan_name
      ,loan_transaction_date
      ,loan_reversal_date
      ,revs_flag
      ,SUM(bankruptcy_flag) AS bankruptcy_flag
      ,SUM(prin_pmt) AS prin_pmt
      ,SUM(int_pmt) AS int_pmt
      ,SUM(prin_refi) AS prin_refi
      ,SUM(int_refi) AS int_refi
      ,SUM(prin_recovery_pmt) AS prin_recovery_pmt
      ,SUM(int_recovery_pmt) AS int_recovery_pmt
      ,SUM(prin_bal_adj) AS prin_bal_adj
      ,SUM(prin_chrg_off) AS prin_chrg_off
      ,SUM(int_chrg_off) AS int_chrg_off
      ,SUM(int_reschd_adj) AS int_reschd_adj
      ,SUM(grs_disb_amt) AS grs_disb_amt
      ,SUM(net_disb_amt) AS net_disb_amt
      ,SUM(int_waived) AS int_waived
  FROM trans_temp
 GROUP BY 1, 2, 3, 4
);


/**** Query application maturity date ****/
CREATE OR REPLACE TEMPORARY TABLE mat_dt_temp AS (

SELECT DISTINCT CAST(application AS VARCHAR(18)) AS app_id
      ,MAX(due_date) AS orig_mat_dt
  FROM loan_dw_prod.cloudlending.amortization_schedule
 WHERE isdeleted = 'FALSE'
   --AND is_archived = 'FALSE'
 GROUP BY 1
);


/**** Creat Full list of Valid Dates for Each Loan ****/
CREATE OR REPLACE TEMPORARY TABLE ball_roll_step1 AS (

SELECT loan.loan_id
      ,dates.calendar_date AS snapshot_dt
      ,loan.loan_start_dt
      ,mat_dt.orig_mat_dt AS loan_maturity_date_orig
      ,TO_DATE(snap.loan_maturity_date_current) AS loan_maturity_date_current
      ,TO_DATE(snap.loan_closed_date) AS loan_closed_dt
      ,loan.int_rate_orig
      ,snap.loan_interest_rate
      ,CASE WHEN dates.calendar_date < (loan.loan_start_dt + 1) THEN 0
       ELSE 1
       END AS loan_ammort_flag
      ,CASE WHEN snap.loan_closed_date IS NOT NULL AND dates.calendar_date > TO_DATE(snap.loan_closed_date) THEN 0
       ELSE 1
       END AS loan_active_flag
  FROM dim_temp AS loan
  LEFT JOIN (SELECT loan_id
                   ,MIN(loan_transaction_date) AS disb_dt
               FROM disb_trans_temp
              GROUP BY 1
                    ) AS disb
    ON loan.loan_id = disb.loan_id
  JOIN mart_finance.lgilius.fin_fpa_dt_dim AS dates
    ON calendar_date >= disb.disb_dt
   AND TO_DATE(CURRENT_TIMESTAMP()) > calendar_date    
  LEFT JOIN mat_dt_temp AS mat_dt
    ON loan.app_id = mat_dt.app_id
  LEFT JOIN (SELECT *
               FROM loan_dw_prod.cloudlending.snapshot_loan_account
              WHERE snapshot_dt = (SELECT MAX(snapshot_dt) 
                                     FROM loan_dw_prod.cloudlending.snapshot_loan_account)
                    ) AS snap
    ON snap.id = loan.loan_id
);


/**** Append transaction data ****/
CREATE OR REPLACE TEMPORARY TABLE ball_roll_step2 AS (

SELECT step1.loan_id
      ,step1.snapshot_dt
      ,step1.loan_closed_dt
      ,step1.int_rate_orig
      ,step1.loan_interest_rate
      ,step1.loan_active_flag
      ,ZEROIFNULL(tran.grs_disb_amt) AS grs_disb_amt
      ,ZEROIFNULL(tran.net_disb_amt) AS net_disb_amt
      ,ZEROIFNULL(tran.prin_pmt) AS prin_pmt
      ,ZEROIFNULL(tran.int_pmt) AS int_pmt
      ,ZEROIFNULL(tran.prin_refi) AS prin_refi
      ,ZEROIFNULL(tran.int_refi) AS int_refi
      ,ZEROIFNULL(tran.prin_recovery_pmt) AS prin_recovery_pmt
      ,ZEROIFNULL(tran.int_recovery_pmt) AS int_recovery_pmt
      ,ZEROIFNULL(tran.prin_bal_adj) AS prin_bal_adj
      ,ZEROIFNULL(tran.prin_chrg_off) AS prin_chrg_off
      ,ZEROIFNULL(tran.int_chrg_off) AS int_chrg_off
      ,ZEROIFNULL(tran.int_reschd_adj) AS int_reschd_adj
  FROM ball_roll_step1 AS step1
  LEFT JOIN (SELECT *
               FROM mart_finance.lgilius.loan_transactions
              WHERE revs_flag = 0
                    ) AS tran
    ON step1.loan_id = tran.loan_id
   AND step1.snapshot_dt = tran.loan_transaction_date
);


/**** Create cummulative fields for each transaction for balance roll calcs ****/
CREATE OR REPLACE TEMPORARY TABLE ball_roll_step3 AS (
SELECT loan_id
      ,snapshot_dt
      ,loan_closed_dt
      ,int_rate_orig
      ,loan_interest_rate
      ,loan_active_flag
      ,grs_disb_amt
      --,net_disb_amt
      ,prin_pmt
      ,int_pmt
      ,prin_bal_adj
      ,prin_refi
      ,int_refi
      ,prin_chrg_off
      ,int_chrg_off
      ,prin_recovery_pmt
      ,int_recovery_pmt
      ,int_reschd_adj
      ,SUM(grs_disb_amt) OVER (PARTITION BY loan_id ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_grs_disb_amt
      --,SUM(net_disb_amt) OVER (PARTITION BY loan_id ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_net_disb_amt
      ,SUM(prin_pmt) OVER (PARTITION BY loan_id ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_prin_pmt
      ,SUM(int_pmt) OVER (PARTITION BY loan_id ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_int_pmt
      ,SUM(prin_bal_adj) OVER (PARTITION BY loan_id ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_prin_bal_adj
      ,SUM(prin_refi) OVER (PARTITION BY loan_id ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_prin_refi
      ,SUM(int_refi) OVER (PARTITION BY loan_id ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_int_refi
      ,SUM(prin_chrg_off) OVER (PARTITION BY loan_id ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_prin_chrg_off
      ,SUM(int_chrg_off) OVER (PARTITION BY loan_id ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_int_chrg_off
      ,SUM(prin_recovery_pmt) OVER (PARTITION BY loan_id ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_prin_recovery_pmt
      ,SUM(int_recovery_pmt) OVER (PARTITION BY loan_id ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_int_recovery_pmt
      ,SUM(int_reschd_adj) OVER (PARTITION BY loan_id ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_int_reschd_adj
  FROM ball_roll_step2
);


/**** This section adjusts the incorrect transactions ****/

CREATE OR REPLACE TEMPORARY TABLE co_date_temp AS (
SELECT loan_id
      ,MAX(loan_transaction_date)  AS co_date
  FROM mart_finance.lgilius.loan_transactions
 WHERE prin_chrg_off > 0 OR int_chrg_off > 0
 GROUP BY 1
);


CREATE OR REPLACE TEMPORARY TABLE refi_date_temp AS (
SELECT loan_id
      ,MAX(loan_transaction_date)  AS refi_date
  FROM mart_finance.lgilius.loan_transactions
 WHERE prin_refi > 0 OR int_refi > 0
 GROUP BY 1
);


CREATE OR REPLACE TEMPORARY TABLE max_pmt_date_temp AS (
SELECT loan_id
      ,MAX(loan_transaction_date)  AS max_pmt_date
  FROM mart_finance.lgilius.loan_transactions
 WHERE prin_pmt > 0 OR int_pmt > 0
 GROUP BY 1
);


CREATE OR REPLACE TEMPORARY TABLE max_tran_temp AS (
SELECT loan_id
      ,loan_closed_dt
      ,cumm_grs_disb_amt
      ,cumm_prin_pmt
      ,cumm_int_pmt
      ,cumm_prin_bal_adj
      ,cumm_prin_refi
      ,cumm_int_refi
      ,cumm_prin_chrg_off
      ,cumm_int_chrg_off
      ,cumm_int_reschd_adj
  FROM ball_roll_step3
 WHERE snapshot_dt = (SELECT MAX(snapshot_dt) FROM ball_roll_step3)
);


CREATE OR REPLACE TEMPORARY TABLE co_temp AS (
SELECT dt.loan_id
      ,dt.co_date
      ,(cumm_grs_disb_amt - cumm_prin_pmt - cumm_prin_bal_adj - cumm_prin_refi) AS prin_chrg_off
      ,(-cumm_int_pmt - cumm_int_refi - cumm_int_reschd_adj) AS int_chrg_off_step
      ,cumm_prin_chrg_off AS prin_chrg_off_cl
      ,cumm_int_chrg_off AS int_chrg_off_cl
  FROM co_date_temp AS dt
  LEFT JOIN max_tran_temp AS tran
    ON dt.loan_id = tran.loan_id
);


CREATE OR REPLACE TEMPORARY TABLE refi_temp AS (
SELECT dt.loan_id
      ,dt.refi_date
      ,(cumm_grs_disb_amt - cumm_prin_pmt - cumm_prin_bal_adj - cumm_prin_chrg_off) AS prin_refi
      ,(-cumm_int_pmt - cumm_int_chrg_off - cumm_int_reschd_adj) AS int_refi_step
      ,cumm_prin_refi AS prin_refi_cl
      ,cumm_int_refi AS int_refi_cl
  FROM refi_date_temp AS dt
  LEFT JOIN max_tran_temp AS tran
    ON dt.loan_id = tran.loan_id
);


CREATE OR REPLACE TEMPORARY TABLE closed_pmt_temp AS (
SELECT dt.loan_id
      ,dt.max_pmt_date
      ,loan_closed_dt
      ,CASE WHEN tran.loan_closed_dt IS NOT NULL THEN (cumm_grs_disb_amt - cumm_prin_refi - cumm_prin_bal_adj - cumm_prin_chrg_off)
       ELSE cumm_prin_pmt
       END AS prin_pmt
      ,CASE WHEN tran.loan_closed_dt IS NOT NULL THEN (-cumm_int_refi - cumm_int_chrg_off - cumm_int_reschd_adj)
       ELSE cumm_int_pmt
       END AS int_pmt_step
      ,cumm_prin_pmt AS prin_pmt_cl
      ,cumm_int_pmt AS int_pmt_cl
  FROM max_pmt_date_temp AS dt
 INNER JOIN max_tran_temp AS tran
    ON dt.loan_id = tran.loan_id
  LEFT JOIN co_date_temp AS co
    ON dt.loan_id = co.loan_id
  LEFT JOIN refi_date_temp AS refi
    ON dt.loan_id = refi.loan_id
 WHERE (co.co_date IS NULL OR refi.refi_date IS NULL)
   AND loan_closed_dt IS NOT NULL
);


CREATE OR REPLACE TEMPORARY TABLE ball_roll_step4 AS (
SELECT step3.loan_id
      ,step3.snapshot_dt
      ,step3.loan_closed_dt
      ,step3.int_rate_orig
      ,step3.loan_interest_rate
      ,step3.loan_active_flag
      ,grs_disb_amt
      --,net_disb_amt
      ,CASE WHEN step3.snapshot_dt = clsd.max_pmt_date THEN clsd.prin_pmt - prev_pmt.cumm_prin_pmt
       ELSE step3.prin_pmt
       END AS prin_pmt
      ,CASE WHEN step3.snapshot_dt = clsd.max_pmt_date THEN clsd.prin_pmt_cl - prev_pmt.cumm_prin_pmt
       ELSE 0
       END AS prin_pmt_cl
      ,step3.int_pmt
      ,CASE WHEN step3.snapshot_dt = clsd.max_pmt_date THEN clsd.int_pmt_step
       ELSE 0
       END AS int_pmt_step
      ,CASE WHEN step3.snapshot_dt = clsd.max_pmt_date THEN clsd.int_pmt_cl
       ELSE 0
       END AS int_pmt_cl
      ,prin_bal_adj
      ,CASE WHEN step3.snapshot_dt = refi.refi_date THEN refi.prin_refi
       ELSE 0
       END AS prin_refi
      ,CASE WHEN step3.snapshot_dt = refi.refi_date THEN refi.prin_refi_cl
       ELSE 0
       END AS prin_refi_cl
      ,CASE WHEN step3.snapshot_dt = refi.refi_date THEN refi.int_refi_step
       ELSE 0
       END AS int_refi_step
      ,CASE WHEN step3.snapshot_dt = refi.refi_date THEN refi.int_refi_cl
       ELSE 0
       END AS int_refi_cl
      ,CASE WHEN step3.snapshot_dt = co.co_date THEN co.prin_chrg_off
       ELSE 0
       END AS prin_chrg_off
      ,CASE WHEN step3.snapshot_dt = co.co_date THEN co.prin_chrg_off_cl
       ELSE 0
       END AS prin_chrg_off_cl
      ,CASE WHEN step3.snapshot_dt = co.co_date THEN co.int_chrg_off_step
       ELSE 0
       END AS int_chrg_off_step
      ,CASE WHEN step3.snapshot_dt = co.co_date THEN co.int_chrg_off_cl
       ELSE 0
       END AS int_chrg_off_cl
      ,prin_recovery_pmt
      ,int_recovery_pmt
      ,int_reschd_adj
      ,cumm_grs_disb_amt
      --,cumm_net_disb_amt
      ,CASE WHEN step3.snapshot_dt >= clsd.max_pmt_date THEN clsd.prin_pmt
       ELSE step3.cumm_prin_pmt
       END AS cumm_prin_pmt
      ,cumm_int_pmt
      ,CASE WHEN step3.snapshot_dt >= clsd.max_pmt_date THEN clsd.int_pmt_step
       ELSE 0
       END AS cumm_int_pmt_step
      ,cumm_prin_bal_adj
      ,CASE WHEN step3.snapshot_dt >= refi.refi_date THEN refi.prin_refi
       ELSE cumm_prin_refi
       END AS cumm_prin_refi
      ,cumm_int_refi
      ,CASE WHEN step3.snapshot_dt >= refi.refi_date THEN refi.int_refi_step
       ELSE 0
       END AS cumm_int_refi_step
      ,CASE WHEN step3.snapshot_dt >= co.co_date THEN co.prin_chrg_off
       ELSE cumm_prin_chrg_off
       END AS cumm_prin_chrg_off
      ,cumm_int_chrg_off
      ,CASE WHEN step3.snapshot_dt >= co.co_date THEN co.int_chrg_off_step
       ELSE 0
       END AS cumm_int_chrg_off_step
      ,cumm_prin_recovery_pmt
      ,cumm_int_recovery_pmt
      ,cumm_int_reschd_adj
  FROM ball_roll_step3 AS step3
  LEFT JOIN co_temp AS co
    ON step3.loan_id = co.loan_id
  LEFT JOIN refi_temp AS refi
    ON step3.loan_id = refi.loan_id
  LEFT JOIN closed_pmt_temp AS clsd
    ON step3.loan_id = clsd.loan_id
  LEFT JOIN (SELECT loan_id
                   ,DATEADD(DAY, 1, snapshot_dt) AS snapshot_dt
                   ,cumm_prin_pmt
               FROM ball_roll_step3
                    ) AS prev_pmt
    ON step3.loan_id = prev_pmt.loan_id
   AND step3.snapshot_dt = prev_pmt.snapshot_dt
);


/**** Calculate the principal balance roll ****/
CREATE OR REPLACE TEMPORARY TABLE ball_roll_prin AS (
SELECT loan_id
      ,snapshot_dt
      ,loan_closed_dt
      ,(loan_active_flag * ((cumm_grs_disb_amt - cumm_prin_pmt - cumm_prin_bal_adj - cumm_prin_chrg_off - cumm_prin_refi) - grs_disb_amt + 
        prin_pmt + prin_bal_adj + prin_chrg_off + prin_refi)) AS prin_beg_bal
      ,(loan_active_flag * grs_disb_amt) AS grs_disb_amt
      ,(loan_active_flag * prin_pmt) AS prin_pmt
      ,(loan_active_flag * prin_bal_adj) AS prin_bal_adj
      ,(loan_active_flag * prin_refi) AS prin_refi
      ,(loan_active_flag * prin_chrg_off) AS prin_chrg_off
      ,(loan_active_flag * (cumm_grs_disb_amt - cumm_prin_pmt - cumm_prin_bal_adj - cumm_prin_chrg_off - cumm_prin_refi)) AS prin_end_bal
  FROM ball_roll_step4
);


/*
CREATE OR REPLACE TABLE mart_finance.lgilius.temp_ball_roll_prin_v3 AS (
SELECT *
  FROM ball_roll_prin
);
*/


/**** Calculate Daily Interest Rates ****/
CREATE OR REPLACE TEMPORARY TABLE int_bal_step1 AS (
SELECT prin.loan_id
      ,prin.snapshot_dt
      ,prin.prin_beg_bal
      ,CASE WHEN int_hist.curr_int_rate IS NULL THEN loans.int_rate_orig
       ELSE int_hist.curr_int_rate
       END AS curr_int_rate
      ,((CASE WHEN int_hist.curr_int_rate IS NULL AND prin.snapshot_dt < '2021-01-01' THEN loans.int_rate_orig/365
       WHEN int_hist.curr_int_rate IS NULL AND prin.snapshot_dt >= '2021-01-01' THEN loans.int_rate_orig/dts.days_in_year
       WHEN prin.snapshot_dt < '2021-01-01' THEN int_hist.curr_int_rate/365
       ELSE int_hist.curr_int_rate/dts.days_in_year
       END) /100) AS curr_daily_int_rate
      ,CASE WHEN prin.snapshot_dt < '2021-01-01' THEN '365'
       ELSE '365/366'
       END AS int_rate_daily_metric
      ,dts.days_in_year
      ,step1.loan_ammort_flag
      ,step4.loan_active_flag
      ,step4.int_pmt
      ,step4.int_pmt_step
      ,step4.int_refi_step
      ,step4.int_chrg_off_step
      ,step4.int_reschd_adj
      ,step4.cumm_int_pmt
      ,step4.cumm_int_pmt_step
      ,step4.cumm_int_refi
      ,step4.cumm_int_refi_step
      ,step4.cumm_int_chrg_off
      ,step4.cumm_int_chrg_off_step
      ,step4.cumm_int_reschd_adj
  FROM ball_roll_prin AS prin
  LEFT JOIN mart_finance.lgilius.int_rate_history AS int_hist
    ON prin.loan_id = int_hist.loan_id
   AND prin.snapshot_dt >= int_hist.start_dt
   AND prin.snapshot_dt <= int_hist.end_dt
  LEFT JOIN (SELECT DISTINCT year
                   ,days_in_year
               FROM mart_finance.lgilius.fin_fpa_dt_dim
                    ) AS dts
    ON EXTRACT(YEAR FROM prin.snapshot_dt) = dts.year
  LEFT JOIN dim_temp AS loans
    ON prin.loan_id = loans.loan_id
  LEFT JOIN ball_roll_step4 AS step4
    ON prin.loan_id = step4.loan_id
   AND prin.snapshot_dt = step4.snapshot_dt
  LEFT JOIN ball_roll_step1 AS step1
    ON prin.loan_id = step1.loan_id
   AND prin.snapshot_dt = step1.snapshot_dt
);


/**** Calculate Interest Accrual Cummulative ****/
CREATE OR REPLACE TEMPORARY TABLE int_bal_step2 AS (
SELECT *
      ,(prin_beg_bal * curr_daily_int_rate * loan_ammort_flag * loan_active_flag) AS int_charged
      ,SUM(prin_beg_bal * curr_daily_int_rate * loan_ammort_flag * loan_active_flag) OVER (PARTITION BY loan_id ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_int_charged
  FROM int_bal_step1
);


CREATE OR REPLACE TEMPORARY TABLE max_int_bal AS (
SELECT loan_id
      ,cumm_int_charged AS tot_int_charged
      ,cumm_int_pmt AS tot_int_pmt
      ,cumm_int_refi AS tot_int_refi
      ,cumm_int_chrg_off AS tot_int_chrg_off
      ,cumm_int_reschd_adj AS tot_int_reschd_adj
  FROM int_bal_step2
 WHERE snapshot_dt = (SELECT MAX(snapshot_dt) FROM int_bal_step2)
);


CREATE OR REPLACE TEMPORARY TABLE int_bal_prev AS (
SELECT loan_id
      ,DATEADD(DAY, 1, snapshot_dt) AS snapshot_dt
      ,cumm_int_pmt
  FROM int_bal_step2
);


/**** Calculate Adjusted End of Loan Values ****/
CREATE OR REPLACE TEMPORARY TABLE int_bal_step3 AS (
SELECT step2.loan_id
      ,step2.snapshot_dt
      ,prin_beg_bal
      ,curr_int_rate
      ,curr_daily_int_rate
      ,int_rate_daily_metric
      ,days_in_year
      ,loan_ammort_flag
      ,loan_active_flag
      ,CASE WHEN int_pmt_step = 0 THEN int_pmt
       WHEN (max_int.tot_int_refi > 0 OR max_int.tot_int_chrg_off > 0) THEN int_pmt
       ELSE (step2.cumm_int_charged - prev.cumm_int_pmt + int_pmt_step)
       END AS int_pmt
      ,CASE WHEN int_refi_step = 0 THEN 0
       ELSE (max_int.tot_int_charged + int_refi_step)
       END AS int_refi
      ,CASE WHEN int_chrg_off_step = 0 THEN 0
       ELSE (max_int.tot_int_charged + int_chrg_off_step)
       END AS int_chrg_off
      ,int_reschd_adj
      ,int_charged
      ,CASE WHEN cumm_int_pmt_step = 0 THEN step2.cumm_int_pmt
       WHEN (max_int.tot_int_refi > 0 OR max_int.tot_int_chrg_off > 0) THEN step2.cumm_int_pmt
       ELSE (max_int.tot_int_charged + cumm_int_pmt_step)
       END AS cumm_int_pmt
      ,CASE WHEN cumm_int_refi_step = 0 THEN 0
       ELSE (max_int.tot_int_charged + cumm_int_refi_step)
       END AS cumm_int_refi
      ,CASE WHEN cumm_int_chrg_off_step = 0 THEN 0
       ELSE (max_int.tot_int_charged + cumm_int_chrg_off_step)
       END AS cumm_int_chrg_off
      ,cumm_int_reschd_adj
      ,step2.cumm_int_charged
  FROM int_bal_step2 AS step2
  LEFT JOIN max_int_bal AS max_int
    ON step2.loan_id = max_int.loan_id
  LEFT JOIN int_bal_prev AS prev
    ON step2.loan_id = prev.loan_id
   AND step2.snapshot_dt = prev.snapshot_dt
);


/**** Interest Balance Roll ****/
CREATE OR REPLACE TEMPORARY TABLE ball_roll_int AS (
SELECT loan_id
      ,snapshot_dt
      ,curr_int_rate
      ,curr_daily_int_rate
      ,int_rate_daily_metric
      ,loan_ammort_flag
      ,((cumm_int_charged - cumm_int_pmt - cumm_int_chrg_off - cumm_int_refi - cumm_int_reschd_adj) -
        int_charged + int_pmt + int_chrg_off + int_refi + int_reschd_adj) AS int_beg_bal
      ,int_charged
      ,int_pmt
      ,int_chrg_off
      ,int_refi
      ,int_reschd_adj
      ,(cumm_int_charged - cumm_int_pmt - cumm_int_chrg_off - cumm_int_refi - cumm_int_reschd_adj) AS int_end_bal
  FROM int_bal_step3
);


/*
CREATE OR REPLACE TABLE mart_finance.lgilius.temp_ball_roll_int_v3 AS (
SELECT *
  FROM ball_roll_int
);
*/

/**** Need to zero out everything after close ****/
/* TODO: Fix all closure dates */
CREATE OR REPLACE TEMPORARY TABLE ball_roll_temp AS (
SELECT prin.loan_id
      ,prin.snapshot_dt
      ,prin.loan_closed_dt
      ,co.co_date AS charge_off_date
      ,refi.refi_date
      ,int.curr_int_rate
      ,int.curr_daily_int_rate
      ,int.int_rate_daily_metric
      ,prin.prin_beg_bal AS prin_beg_bal
      ,prin.grs_disb_amt AS grs_disb_amt
      ,prin.prin_pmt AS prin_pmt
      ,prin.prin_bal_adj AS prin_bal_adj
      ,prin.prin_refi AS prin_refi
      ,prin.prin_chrg_off AS prin_chrg_off
      ,prin.prin_end_bal AS prin_end_bal
      ,int.int_beg_bal AS int_beg_bal
      ,int.int_charged AS int_charged
      ,int.int_pmt AS int_pmt
      ,int.int_chrg_off AS int_chrg_off
      ,int.int_refi AS int_refi
      ,int.int_reschd_adj AS int_reschd_adj
      ,int.int_end_bal AS int_end_bal
      ,prin.prin_chrg_off AS prin_chrg_off_db
      ,int.int_chrg_off AS int_chrg_off_db
  FROM ball_roll_prin AS prin
  LEFT JOIN ball_roll_int AS int
    ON prin.loan_id = int.loan_id
   AND prin.snapshot_dt = int.snapshot_dt
  LEFT JOIN co_date_temp AS co
    ON prin.loan_id = co.loan_id
  LEFT JOIN refi_date_temp AS refi
    ON prin.loan_id = refi.loan_id
);


/**** DPD estimates ****/

/**** Creat table of scheduled payments ****/
CREATE OR REPLACE TEMPORARY TABLE schd_pmt_temp AS (
SELECT loan_id
      ,schd_pmt_dt
      ,schd_pmt_amt
      ,ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY schd_pmt_dt ASC) AS pmt_number
  FROM (SELECT CAST(loan_account AS VARCHAR(18)) AS loan_id
              ,createddate
              ,TO_DATE(loan_due_date) AS schd_pmt_dt
              ,loan_total_installment AS schd_pmt_amt
              ,ROW_NUMBER() OVER (PARTITION BY loan_account, loan_due_date ORDER BY createddate DESC) AS adj_order
          FROM loan_dw_prod.cloudlending.loan_repayment_schedule
               ) AS schd_data
 WHERE adj_order = 1
);


/**** Create Start and End Dates ****/
CREATE OR REPLACE TEMPORARY TABLE dpd_temp_step1 AS (
SELECT curr.loan_id
      ,CASE WHEN prev.schd_pmt_dt IS NULL THEN '1990-12-31'
       ELSE DATEADD(DAY, 1, prev.schd_pmt_dt)
       END AS start_dt
      ,curr.schd_pmt_dt AS end_dt
      ,curr.schd_pmt_amt
      ,curr.pmt_number
  FROM schd_pmt_temp AS curr
  LEFT JOIN schd_pmt_temp AS prev
    ON curr.loan_id = prev.loan_id
   AND (curr.pmt_number - 1) = prev.pmt_number
);


CREATE OR REPLACE TEMPORARY TABLE start_dt_temp AS (
SELECT loan_id
      ,MIN(snapshot_dt) AS loan_start_dt
  FROM ball_roll_step2
 GROUP BY 1
);


/**** Join with Payment Data ****/
CREATE OR REPLACE TEMPORARY TABLE dpd_temp_step2 AS (
SELECT bal.loan_id
      ,bal.snapshot_dt
      ,ROUND(bal.prin_pmt, 2) AS prin_pmt
      ,ROUND(bal.int_pmt, 2) AS int_pmt
      ,ROUND(bal.prin_pmt + bal.int_pmt, 2) AS tot_pmt
      ,CASE WHEN dpd1.start_dt = bal.snapshot_dt THEN ROUND(dpd1.schd_pmt_amt, 2)
       WHEN dpd1.start_dt = '1990-12-31' AND bal.snapshot_dt = loan_start_dt THEN ROUND(dpd1.schd_pmt_amt, 2)
       WHEN dpd1.schd_pmt_amt IS NULL THEN 0
       ELSE 0
       END AS schd_pmt_amt
      ,ZEROIFNULL(dpd1.pmt_number) AS pmt_number
  FROM ball_roll_step2 AS bal
  LEFT JOIN dpd_temp_step1 AS dpd1
    ON bal.loan_id = dpd1.loan_id
   AND bal.snapshot_dt >= dpd1.start_dt
   AND bal.snapshot_dt <= dpd1.end_dt
  LEFT JOIN start_dt_temp AS strt_dt
    ON bal.loan_id = strt_dt.loan_id
);


/**** Cumulative Summ of Payment Data by Scheduled Pmt ****/
CREATE OR REPLACE TEMPORARY TABLE dpd_temp_step3 AS (
SELECT loan_id
      ,snapshot_dt
      ,pmt_number
      --,SUM(prin_pmt) OVER (PARTITION BY loan_id, pmt_number ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_prin_pmt
      --,SUM(int_pmt) OVER (PARTITION BY loan_id, pmt_number ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_int_pmt
      ,SUM(tot_pmt) OVER (PARTITION BY loan_id, pmt_number ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_tot_pmt
      ,SUM(schd_pmt_amt) OVER (PARTITION BY loan_id, pmt_number ORDER BY snapshot_dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_schd_pmt_amt
  FROM dpd_temp_step2
);


/**** Determine point of paid in full ****/
CREATE OR REPLACE TEMPORARY TABLE dpd_temp_step4 AS (
SELECT loan_id
      ,snapshot_dt
      ,pmt_number
      ,cumm_tot_pmt
      ,cumm_schd_pmt_amt
      ,CASE WHEN (cumm_tot_pmt - cumm_schd_pmt_amt) >= -0.02 THEN 1 -- 0.02 accounts for small differences in the due amount
       ELSE 0
       END AS paid_in_full_flag
  FROM dpd_temp_step3
);


CREATE OR REPLACE TEMPORARY TABLE dpd_temp_step5 AS (
SELECT pmt.loan_id
      ,pmt.pmt_number
      ,CASE WHEN pf.paid_in_full_flag IS NOT NULL THEN pf.paid_in_full_flag
       ELSE miss.paid_in_full_flag
       END AS paid_in_full_flag
      ,CASE WHEN pf.paid_in_full_flag IS NOT NULL THEN pf.event_dt
       ELSE miss.event_dt
       END AS event_dt
  FROM schd_pmt_temp AS pmt
  LEFT JOIN (SELECT loan_id
	            ,pmt_number
		     ,paid_in_full_flag
		     ,MIN(snapshot_dt) AS event_dt
		     --,cumm_tot_pmt
		     --,cumm_schd_pmt_amt
		 FROM dpd_temp_step4
		WHERE paid_in_full_flag = 1
		  AND cumm_tot_pmt > 0
		GROUP BY 1, 2, 3
		      ) AS pf
    ON pmt.loan_id = pf.loan_id
   AND pmt.pmt_number = pf.pmt_number
  LEFT JOIN (SELECT loan_id
	            ,pmt_number
                  ,paid_in_full_flag
                  ,MAX(snapshot_dt) AS event_dt
                  --,cumm_tot_pmt
                  --,cumm_schd_pmt_amt
                FROM dpd_temp_step4
               WHERE paid_in_full_flag = 0
            	   AND cumm_schd_pmt_amt > 0
             	 GROUP BY 1, 2, 3
             	       ) AS miss
    ON pmt.loan_id = miss.loan_id
   AND pmt.pmt_number = miss.pmt_number
);


/**** Select only periods that changed from the prior ****/

CREATE OR REPLACE TEMPORARY TABLE dpd_temp_step6 AS (
SELECT loan_id
      ,snapshot_dt
      ,paid_in_full_flag
      ,event_flag
      ,ROW_NUMBER() OVER (PARTITION BY loan_id  ORDER BY snapshot_dt ASC) AS event_number
  FROM (SELECT curr.loan_id
	       ,curr.event_dt AS snapshot_dt
	       ,curr.paid_in_full_flag
	       ,CASE WHEN prev.paid_in_full_flag IS NULL THEN 1
	        WHEN curr.paid_in_full_flag <> prev.paid_in_full_flag THEN 1
	        ELSE 0
	        END AS event_flag
	   FROM dpd_temp_step5 AS curr
	   LEFT JOIN dpd_temp_step5 AS prev
	     ON curr.loan_id = prev.loan_id
	    AND (curr.pmt_number - 1) = prev.pmt_number
	        ) AS event
 WHERE event_flag = 1
);



/**** Create date ranges for past due/active changes ****/
CREATE OR REPLACE TEMPORARY TABLE dpd_temp_step7 AS (
SELECT curr.loan_id
      ,curr.snapshot_dt AS start_dt
      ,CASE WHEN nxt.snapshot_dt IS NULL THEN '9999-12-31'
       ELSE DATEADD(DAY, -1, nxt.snapshot_dt)
       END AS end_dt
      ,curr.paid_in_full_flag
      ,curr.event_flag
      ,curr.event_number
  FROM dpd_temp_step6 AS curr
  LEFT JOIN dpd_temp_step6 AS nxt
    ON curr.loan_id = nxt.loan_id
   AND (curr.event_number + 1) = nxt.event_number
);


/**** Calculate DPD and Status ****/
CREATE OR REPLACE TEMPORARY TABLE ball_roll_dpd_temp AS (
SELECT prin.loan_id
      ,prin.snapshot_dt
      ,prin.loan_closed_dt
      ,prin.charge_off_date
      ,prin.refi_date
      ,CASE WHEN prin.snapshot_dt >= prin.charge_off_date THEN 'WRITTEN OFF'
       WHEN prin.snapshot_dt >= prin.refi_date THEN 'REFINANCED'
       WHEN prin.snapshot_dt >= prin.loan_closed_dt THEN 'PAID OFF'
       WHEN dpd7.paid_in_full_flag = 0 THEN 'PAST DUE'
       ELSE 'ACTIVE'
       END AS status
      ,dpd7.paid_in_full_flag
      ,dpd7.event_flag
      ,dpd7.event_number
      ,prin.curr_int_rate
      ,prin.curr_daily_int_rate
      ,prin.int_rate_daily_metric
      ,prin.prin_beg_bal
      ,prin.grs_disb_amt
      ,prin.prin_pmt
      ,prin.prin_bal_adj
      ,prin.prin_refi
      ,prin.prin_chrg_off
      ,prin.prin_end_bal
      ,prin.int_beg_bal
      ,prin.int_charged
      ,prin.int_pmt
      ,prin.int_chrg_off
      ,prin.int_refi
      ,prin.int_reschd_adj
      ,prin.int_end_bal
      ,prin.prin_chrg_off_db
      ,prin.int_chrg_off_db
  FROM ball_roll_temp AS prin
  LEFT JOIN dpd_temp_step7 AS dpd7
    ON prin.loan_id = dpd7.loan_id
   AND prin.snapshot_dt >= dpd7.start_dt
   AND prin.snapshot_dt <= dpd7.end_dt
);


/**** First Payment Date ****/
CREATE OR REPLACE TEMPORARY TABLE frst_pmt_temp AS (
SELECT loan_id
      ,MIN(end_dt) AS first_pmt_due_dt
  FROM dpd_temp_step1
 GROUP BY 1
);


CREATE OR REPLACE TEMPORARY TABLE ball_roll_dpd_temp2 AS (
SELECT bal.loan_id
      ,bal.snapshot_dt
      ,ROW_NUMBER() OVER (PARTITION BY bal.loan_id ORDER BY bal.snapshot_dt ASC) AS days_on_book
      ,loan_closed_dt
      ,charge_off_date
      ,refi_date
      ,status
      ,ROW_NUMBER() OVER (PARTITION BY bal.loan_id, event_number ORDER BY bal.snapshot_dt ASC) AS dpd_temp
      ,CASE WHEN status = 'PAST DUE' AND first_pmt_due_dt = bal.snapshot_dt THEN 1
       ELSE 0
       END AS frst_pmt_miss_flag
      ,paid_in_full_flag
      ,event_flag
      ,curr_int_rate
      ,curr_daily_int_rate
      ,int_rate_daily_metric
      ,prin_beg_bal
      ,grs_disb_amt
      ,prin_pmt
      ,prin_bal_adj
      ,prin_refi
      ,prin_chrg_off
      ,prin_end_bal
      ,int_beg_bal
      ,int_charged
      ,int_pmt
      ,int_chrg_off
      ,int_refi
      ,int_reschd_adj
      ,int_end_bal
      ,prin_chrg_off_cl
      ,int_chrg_off_cl
      ,prin_refi_cl
      ,int_refi_cl
  FROM ball_roll_dpd_temp AS bal
  LEFT JOIN frst_pmt_temp AS frst
    ON bal.loan_id = frst.loan_id
  LEFT JOIN (SELECT loan_id
                   ,snapshot_dt
                   ,prin_chrg_off_cl
                   ,int_chrg_off_cl
                   ,prin_refi_cl
                   ,int_refi_cl
               FROM ball_roll_step4
                    ) AS s4
    ON bal.loan_id = s4.loan_id
   AND bal.snapshot_dt = s4.snapshot_dt
);


CREATE OR REPLACE TABLE mart_finance.lgilius.daily_bal_roll AS (
SELECT loan_id
      ,snapshot_dt
      ,CASE WHEN status IN('WRITTEN OFF', 'REFINANCED', 'PAID OFF') THEN 0
       ELSE days_on_book
       END AS days_on_book
      ,loan_closed_dt
      ,charge_off_date
      ,refi_date
      ,status
      ,CASE WHEN paid_in_full_flag IS NULL THEN 0
       WHEN status IN('ACTIVE', 'WRITTEN OFF', 'REFINANCED', 'PAID OFF') THEN 0
       ELSE (1 - paid_in_full_flag) * dpd_temp
       END AS dpd
      ,frst_pmt_miss_flag
      ,curr_int_rate
      ,curr_daily_int_rate
      ,int_rate_daily_metric
      ,prin_beg_bal
      ,grs_disb_amt
      ,prin_pmt
      ,prin_bal_adj
      ,prin_refi
      ,prin_chrg_off
      ,prin_end_bal
      ,int_beg_bal
      ,int_charged
      ,int_pmt
      ,int_chrg_off
      ,int_refi
      ,int_reschd_adj
      ,int_end_bal
      ,prin_chrg_off_cl
      ,int_chrg_off_cl
      ,prin_refi_cl
      ,int_refi_cl
  FROM ball_roll_dpd_temp2
);
