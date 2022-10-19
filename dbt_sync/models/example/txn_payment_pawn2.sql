{{ config(materialized='view') }}

(
 
SELECT 1 as Entity_ID
      ,E.eTicket_No         AS eTicket_No
      ,E.Original_Ticket_No     AS Original_Ticket_No
      ,E.New_Ticket_No        AS New_Ticket_No
      ,'Self-Service'         AS Payment_Method
      ,E.renewedby_ikiosk_branch    AS Branch_Transaction_Made
      ,E.Ticket_Origin_BranchID   AS Branch_Ticket_Origin
      
      ,O.PaymentGateway AS Payment_Gateway_Detailed 
      ,CASE
        WHEN O.PaymentGateway = 'eNETS'
          THEN 'iPayment'
        WHEN O.PaymentGateway IN ('RedDot','NetsPay')
          THEN 'Mobile'
        ELSE O.PaymentGateway
      END AS Payment_Gateway_Grouped
      ,O.PaymentType AS Payment_Type
      ,E.Interest_Amt AS Interest_Amt
      ,O.TransactionID AS Transaction_ID
      ,O.PaymentGatewayTransactionID AS Payment_Gateway_Transaction_ID
      ,CAST(O.TransactionDateTime AS TIMESTAMP) AS Transaction_Date_Time
      ,CAST(CAST(O.TransactionDateTime AS TIMESTAMP) AS date) AS Transaction_Date
      ,CAST(CAST(O.TransactionDateTime AS TIMESTAMP) AS time) AS Transaction_Time
      ,CurrencyCode AS Currency_Code
      ,AdminSurcharge AS Admin_Surcharge
      ,O.TransactionAmount AS Total_Transaction_Amount
      ,trans_amt.TransactionAmount AS Amount_per_Payment_Gateway_Trans_ID_HEADER_LEVEL
      ,p.Multiplier         AS Payment_Gate_Multiplier
      ,p.AdditionalCharges      AS Payment_Gate_Additional_Charges
      ,COALESCE(FORMAT_TIMESTAMP('%Y%m%d', CAST( O.TransactionDateTime AS TIMESTAMP)), '0') AS _Key_Date_Transaction_Date
      ,COALESCE(CAST(REPLACE(RIGHT(CAST(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', CAST( O.TransactionDateTime AS TIMESTAMP)) AS  STRING), 5), ':', '') AS INT), 0) AS _Key_Time_Transaction_Time 
      -- ,COALESCE(TxnAt.Entity_ID, 1) AS _Key_Branch_Transaction_Made
      -- ,COALESCE(OrigBranch.Entity_ID,1) AS _Key_Branch_Ticket_Origin
      ,CASE
        WHEN OrigBranch.Branch_ID = TxnAt.Branch_ID
          THEN 'Own Store'
        ELSE 'Other Store'
      END AS Payment_for_Store_Type
      ,CASE
        WHEN OrigBranch.Store_Group = TxnAt.Store_Group
          THEN 'Own Group'
        ELSE 'Other Group'
      END AS Payment_for_StoreGroup_Type

      ,'RENEW' AS Transaction_Type
      ,E.Principle_Repayment_Amt AS Downloan_Amt
      ,E.Original_Grant_Amt AS Principal_Amt
      ,(E.Original_Grant_Amt+E.interest_amt) AS Redeem_Amt
      ,AdminSurcharge AS Payment_AdminCharge
      ,0.0 AS Payment_BankCharge
      ,E.Interest_Amt AS Total_Interest_Amt
      ,E.Principle_Repayment_Amt AS Total_Downloan_Amt
      ,E.Original_Grant_Amt AS Total_Principal_Amt
      ,(E.Original_Grant_Amt+E.interest_amt) AS Total_Redeem_Amt

      ,null AS Payment_ID
      ,null AS Payment_Reference
      ,null AS Payment_Mode
      ,null AS Payment_Merchant

  from  aspial-bq.dwh_pawn_mcpdmscentral.tblOnlineTxfRef O 
      INNER JOIN
        aspial-bq.dwh_pawn_mcpdmscentral.tbleTicket E 
      on O.TransactionID = E.TransactionID
      INNER JOIN
        aspial-bq.dwh_pawn_mcpdmscentral.tblPaymentGateway p 
      ON   p.PaymentGateway = CASE
                    WHEN o.PaymentGateway <> 'Ikiosk'
                    THEN  o.PaymentGateway
                    ELSE  o.PaymentType
                  END
      AND CAST(CAST(o.TransactionDateTime AS TIMESTAMP) AS DATE) BETWEEN CAST(cast(p.StartDate as timestamp) AS DATE) AND CAST(cast(p.EndDate as timestamp) AS DATE)
      LEFT JOIN
        (
          SELECT 1 as Entity_ID
              ,TransactionID
              , SUM(Interest_Amt) AS TransactionAmount
          FROM  aspial-bq.dwh_pawn_mcpdmscentral.tbleTicket e 
          group by Entity_ID,TransactionID
        ) as trans_amt
      ON   O.TransactionID = trans_amt.TransactionID
      LEFT JOIN (select distinct Branch_ID, Store_Group  from aspial-bq.datamart_pawn.Master_Branch where Entity_ID = 1 and Store_Group !='NA') 
 TxnAt  ON  E.renewedby_ikiosk_branch = TxnAt.Branch_ID  
  LEFT JOIN (select distinct Branch_ID, Store_Group from    aspial-bq.datamart_pawn.Master_Branch where Entity_ID = 1 and Store_Group !='NA')
 OrigBranch  ON E.Ticket_Origin_BranchID = OrigBranch.Branch_ID

      

  where   #CAST(CAST(transaction_date AS TIMESTAMP) AS DATE) >= Trans_From
     PaymentStatus = 'Successful'
  AND   E.New_Ticket_No IS NOT NULL
  AND   rtrim(e.eTicket_Status) IN ('CONVERTED','COLLECTED')

union all
SELECT  distinct 1 as Entity_ID
      ,SRC.New_ticket_no      AS eTicket_No
      ,SRC.Original_ticket_no   AS Original_Ticket_No
      ,SRC.New_ticket_no      AS New_Ticket_No
      ,'Outlet'     AS Payment_Method
      
      ,SRC.Redeem_Branch_ID AS Branch_Transaction_Made
      ,SRC.BranchID     AS Branch_Ticket_Origin
     
      ,'Counter'      AS Payment_Gateway
      ,'Counter'      AS Payment_Gateway_Grouped
      ,CASE WHEN Payment_PaymentId is null THEN 'CASH' ELSE SRC.Payment_Type END AS Payment_Type
      ,ROUND(
        CASE  
          WHEN Payment_PaymentId is null THEN SRC.interest_amt
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'RENEW' 
            THEN
            CASE WHEN (SRC.interest_amt+SRC.Downloan_Amt)<=Payment_Amt_Exclude_Charge THEN SRC.interest_amt 
            ELSE SRC.interest_amt*Payment_Amt_Exclude_Charge/(SRC.interest_amt+SRC.Downloan_Amt) END
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'REDEEM'  
            THEN
            CASE WHEN SRC.Redeem_Amt<=Payment_Amt_Exclude_Charge THEN SRC.interest_amt 
            ELSE SRC.interest_amt*Payment_Amt_Exclude_Charge/SRC.Redeem_Amt END
          ELSE 0.0 END
        ,2) AS Interest_Amt
      ,concat('TxnID_', SRC.New_ticket_no) AS Transaction_ID
      ,COALESCE(SRC.PaymentId,concat('PymtID_', SRC.New_ticket_no)) AS Payment_Gateway_Transaction_ID
      ,CAST(SRC.transaction_date AS TIMESTAMP) AS Transaction_Date_Time
      ,CAST(CAST(SRC.transaction_date AS TIMESTAMP) AS date) AS Transaction_Date
      ,CAST(CAST(SRC.transaction_date AS TIMESTAMP)AS  time) AS Transaction_Time
      ,'SGD' AS Currency_Code
      ,CASE
          WHEN Payment_PaymentId is null THEN 0.0
          WHEN Payment_PaymentId is not null THEN Payment_AdminCharge
          ELSE 0.0 END
       AS Total_Admin_Surcharge
      ,ROUND(
        CASE
          WHEN Payment_PaymentId is null AND SRC.transaction_Type = 'RENEW' THEN (SRC.interest_amt+SRC.Downloan_Amt)
          WHEN Payment_PaymentId is null AND SRC.transaction_Type = 'REDEEM' THEN SRC.Redeem_Amt 
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'RENEW' THEN Payment_Amt
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'REDEEM' THEN Payment_Amt
          ELSE 0.0 END
        ,2) AS Total_Transaction_Amount
      ,ROUND(
        CASE
          WHEN Payment_PaymentId is null AND SRC.transaction_Type = 'RENEW' THEN (SRC.interest_amt+SRC.Downloan_Amt)
          WHEN Payment_PaymentId is null AND SRC.transaction_Type = 'REDEEM' THEN SRC.Redeem_Amt 
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'RENEW' THEN Payment_Amt_Exclude_Charge
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'REDEEM' THEN Payment_Amt_Exclude_Charge
          ELSE 0.0 END
        ,2) AS Amount_per_Payment_Gateway_Trans_ID_HEADER_LEVEL
      , CASE WHEN SRC.Payment_Type = 'NETS' THEN (SELECT MAX(Multiplier) from aspial-bq.dwh_pawn_mcpdmscentral.tblPaymentGateway where PaymentGateway ='NETS') 
            ELSE 0 END            AS Payment_Gate_Multiplier
      ,0            AS Payment_Gate_Additional_Charges
      ,COALESCE(FORMAT_TIMESTAMP('%Y%m%d', CAST( transaction_date AS TIMESTAMP)), '0') AS _Key_Date_Transaction_Date
      ,COALESCE(CAST(REPLACE(RIGHT(CAST(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', CAST( transaction_date AS TIMESTAMP)) AS  STRING), 5), ':', '') AS INT), 0) AS _Key_Time_Transaction_Time  
      -- ,COALESCE(TxnAt.Entity_ID,1) AS _Key_Branch_Transaction_Made
      -- ,COALESCE(OrigBranch.Entity_ID,1) AS _Key_Branch_Ticket_Origin
      ,CASE
        WHEN OrigBranch.Branch_ID = TxnAt.Branch_ID
          THEN 'Own Store'
        ELSE 'Other Store'
      END AS Payment_for_Store_Type
      ,CASE
        WHEN OrigBranch.Store_Group = TxnAt.Store_Group
          THEN 'Own Group'
        ELSE 'Other Group'
      END AS Payment_for_StoreGroup_Type
      ,SRC.Transaction_Type
      ,ROUND(
        CASE
          WHEN Payment_PaymentId is null AND SRC.transaction_Type = 'RENEW' THEN SRC.Downloan_Amt 
          WHEN Payment_PaymentId is null AND SRC.transaction_Type = 'REDEEM' THEN 0.0
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'RENEW'
            THEN
            CASE WHEN (SRC.interest_amt+SRC.Downloan_Amt)<=Payment_Amt_Exclude_Charge THEN SRC.Downloan_Amt 
            ELSE SRC.Downloan_Amt*Payment_Amt_Exclude_Charge/(SRC.interest_amt+SRC.Downloan_Amt) END
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'REDEEM' THEN 0.0
          ELSE 0.0 END 
        ,2) AS Downloan_Amt
      ,ROUND(
        CASE
          WHEN Payment_PaymentId is null THEN SRC.grant_amt 
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'RENEW'
            THEN
            CASE WHEN (SRC.interest_amt+SRC.Downloan_Amt)<=Payment_Amt_Exclude_Charge THEN SRC.grant_amt 
            ELSE SRC.grant_amt*Payment_Amt_Exclude_Charge/(SRC.interest_amt+SRC.Downloan_Amt) END
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'REDEEM' 
            THEN
            CASE WHEN SRC.redeem_amt<=Payment_Amt_Exclude_Charge THEN SRC.grant_amt
            ELSE SRC.grant_amt*Payment_Amt_Exclude_Charge/SRC.Redeem_Amt END
          ELSE 0.0 END 
        ,2) AS Principal_Amt
      ,ROUND(
        CASE
          WHEN Payment_PaymentId is null THEN SRC.Redeem_Amt 
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'RENEW'
            THEN
            CASE WHEN (SRC.interest_amt+SRC.Downloan_Amt)<=Payment_Amt_Exclude_Charge THEN SRC.Redeem_Amt 
            ELSE SRC.Redeem_Amt*Payment_Amt_Exclude_Charge/(SRC.interest_amt+SRC.Downloan_Amt) END
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'REDEEM'
            THEN
            CASE WHEN SRC.redeem_amt<=Payment_Amt_Exclude_Charge THEN SRC.Redeem_Amt
            ELSE Payment_Amt_Exclude_Charge END
          ELSE 0.0 END 
        ,2) AS Redeem_Amt
      ,ROUND(
        CASE  
          WHEN Payment_PaymentId is null THEN 0.0
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'RENEW' 
            THEN
            CASE WHEN (SRC.interest_amt+SRC.Downloan_Amt)<Payment_Amt_Exclude_Charge THEN Payment_AdminCharge*(SRC.interest_amt+SRC.Downloan_Amt)/Payment_Amt_Exclude_Charge
            ELSE Payment_AdminCharge END
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'REDEEM'
            THEN
            CASE WHEN SRC.redeem_amt<Payment_Amt_Exclude_Charge THEN Payment_AdminCharge*SRC.redeem_amt/Payment_Amt_Exclude_Charge
            ELSE Payment_AdminCharge END
          ELSE 0.0 END 
        ,2) AS Payment_AdminCharge
      ,ROUND(
        CASE  
          WHEN Payment_PaymentId is null THEN 0.0
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'RENEW' 
            THEN
            CASE WHEN (SRC.interest_amt+SRC.Downloan_Amt)<Payment_Amt_Exclude_Charge THEN Payment_BankCharge*(SRC.interest_amt+SRC.Downloan_Amt)/Payment_Amt_Exclude_Charge
            ELSE Payment_BankCharge END
          WHEN Payment_PaymentId is not null AND SRC.transaction_Type = 'REDEEM'
            THEN
            CASE WHEN SRC.redeem_amt<Payment_Amt_Exclude_Charge THEN Payment_BankCharge*SRC.redeem_amt/Payment_Amt_Exclude_Charge
            ELSE Payment_BankCharge END
          ELSE 0.0 END
        ,2) AS Payment_BankCharge
      ,SRC.Interest_Amt AS Total_Interest_Amt
      ,SRC.Downloan_Amt AS Total_Downloan_Amt
      ,SRC.grant_amt AS Total_Principal_Amt
      ,SRC.Redeem_Amt AS Total_Redeem_Amt
      ,SRC.PaymentId AS Payment_ID
      ,SRC.Payment_Reference AS Payment_Reference
      ,SRC.Payment_Mode AS Payment_Mode
      ,SRC.Payment_Merchant AS Payment_Merchant
  FROM (      
    SELECT SRC.*
    ,Tic.grant_amt AS New_Grant_Amt
    , CASE WHEN Tic.grant_amt is not null and SRC.grant_amt>COALESCE(Tic.grant_amt,0.0) THEN SRC.grant_amt-COALESCE(Tic.grant_amt,0.0) ELSE 0.0 END AS Downloan_Amt
    ,P.Reference AS Payment_Reference
    ,Pt.ReportAlias AS Payment_Type
    ,Pm.ReportAlias AS Payment_Mode
    ,Pm.Merchant AS Payment_Merchant
    ,P.PaymentId AS Payment_PaymentId
    ,COALESCE(P.Amount,0.0) AS Payment_Amt
    ,COALESCE(P.Amount,0.0)-COALESCE(cast(json_extract_scalar(ExtraDetails,'$.Charges.AdminCharge' ) as float64),0.0) AS Payment_Amt_Exclude_Charge
    ,COALESCE(cast(json_extract_scalar(ExtraDetails,'$.Charges.AdminCharge' )as float64),0.0) AS Payment_AdminCharge
    ,COALESCE(cast(json_extract_scalar(ExtraDetails,'$.Charges.Charge' )as float64),0.0) AS Payment_BankCharge
      FROM  aspial-bq.dwh_pawn_mcpdmscentral.tblRedeem_Renew SRC  
          LEFT JOIN aspial-bq.dwh_pawn_mcpdmscentral.tblTicket Tic on  SRC.New_ticket_no=Tic.ticket_no
          LEFT JOIN aspial-bq.dwh_pawn_mcpdmscentral.tblPayment P  on  SRC.PaymentId=P.PaymentId
          LEFT JOIN aspial-bq.dwh_pawn_mcpdmscentral.tblPaymentType Pt  on  P.PayTypeId=Pt.PayTypeId

          LEFT JOIN aspial-bq.dwh_pawn_mcpdmscentral.tblPaymentMode Pm  on  P.PayModeId=Pm.PayModeId
      WHERE RTRIM(SRC.transaction_status) = 'VALID'
      #AND   CAST(CAST(SRC.transaction_date AS TIMESTAMP) AS DATE) >= Trans_From
      AND   SRC.CreatedBy NOT IN ('IKIOSK','IPAYMENT','MPAYMENT')
  ) SRC
  LEFT JOIN (select distinct Branch_ID, Store_Group from aspial-bq.datamart_pawn.Master_Branch where Entity_ID = 1 and Store_Group !='NA') 
 TxnAt  ON  SRC.Redeem_Branch_ID = TxnAt.Branch_ID  
  LEFT JOIN (select distinct Branch_ID, Store_Group from  aspial-bq.datamart_pawn.Master_Branch where Entity_ID = 1 and Store_Group !='NA')
 OrigBranch  ON SRC.BranchID = OrigBranch.Branch_ID 

union all

SELECT	SRC.Entity_ID
			,SRC.RenewTicketNo			AS eTicket_No
			,SRC.OriginTicketNo		AS Original_Ticket_No
			,SRC.RenewTicketNo			AS New_Ticket_No
			,'Outlet'			AS Payment_Method
			,SRC.RedeemBranchId	AS Branch_Transaction_Made
			,SRC.OriginBranchId			AS Branch_Ticket_Origin
			,'Counter'			AS Payment_Gateway
			,'Counter'			AS Payment_Gateway_Grouped
			,CASE WHEN Payment_PaymentId is null THEN 'CASH' ELSE SRC.Payment_Type END AS Payment_Type
			,ROUND(
				CASE	
					WHEN Payment_PaymentId is null THEN SRC.InterestAmount
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'RENEW' 
						THEN
						CASE WHEN (SRC.InterestAmount+SRC.Downloan_Amt)<=Payment_Amt_Exclude_Charge THEN SRC.InterestAmount 
						ELSE SRC.InterestAmount*Payment_Amt_Exclude_Charge/(SRC.InterestAmount+SRC.Downloan_Amt) END
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'REDEEM'  
						THEN
						CASE WHEN SRC.RedeemAmount<=Payment_Amt_Exclude_Charge THEN SRC.InterestAmount 
						ELSE SRC.InterestAmount*Payment_Amt_Exclude_Charge/SRC.RedeemAmount END
					ELSE 0.0 END
				,2) AS Interest_Amt
			,concat('TxnID_', SRC.RenewTicketNo) AS Transaction_ID
			,COALESCE(cast(SRC.ReceiptId as string),concat('PymtID_', SRC.RenewTicketNo)) AS Payment_Gateway_Transaction_ID
			,CAST(SRC.TransactionDate AS timestamp) AS Transaction_Date_Time
			,CAST(cast(SRC.TransactionDate as timestamp) AS date) AS Transaction_Date
			,CAST(cast(SRC.TransactionDate as timestamp) AS time) AS Transaction_Time
			,'SGD' AS Currency_Code
			,CASE
					WHEN Payment_PaymentId is null THEN 0.0
					WHEN Payment_PaymentId is not null THEN Payment_AdminCharge
					ELSE 0.0 END
			 AS Total_Admin_Surcharge
			,ROUND(
				CASE
					WHEN Payment_PaymentId is null AND SRC.TransactionType = 'RENEW' THEN (SRC.InterestAmount+SRC.Downloan_Amt)
					WHEN Payment_PaymentId is null AND SRC.TransactionType = 'REDEEM' THEN SRC.RedeemAmount 
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'RENEW' THEN Payment_Amt
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'REDEEM' THEN Payment_Amt
					ELSE 0.0 END
				,2) AS Total_Transaction_Amount
			,ROUND(
				CASE
					WHEN Payment_PaymentId is null AND SRC.TransactionType = 'RENEW' THEN (SRC.InterestAmount+SRC.Downloan_Amt)
					WHEN Payment_PaymentId is null AND SRC.TransactionType = 'REDEEM' THEN SRC.RedeemAmount 
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'RENEW' THEN Payment_Amt_Exclude_Charge
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'REDEEM' THEN Payment_Amt_Exclude_Charge
					ELSE 0.0 END
				,2) AS Amount_per_Payment_Gateway_Trans_ID_HEADER_LEVEL
			, CASE WHEN SRC.Payment_Type = 'NETS' THEN (SELECT MAX(Multiplier) from aspial-bq.dwh_pawn_mcpdmscentral.tblPaymentGateway where PaymentGateway ='NETS') 
            ELSE 0 END            AS Payment_Gate_Multiplier
      ,0            AS Payment_Gate_Additional_Charges
      ,COALESCE(FORMAT_TIMESTAMP('%Y%m%d', CAST( TransactionDate AS TIMESTAMP)), '0') AS _Key_Date_Transaction_Date
      ,COALESCE(CAST(REPLACE(RIGHT(CAST(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', CAST( TransactionDate AS TIMESTAMP)) AS  STRING), 5), ':', '') AS INT), 0) AS _Key_Time_Transaction_Time  

			-- ,ISNULL(TxnAt._Key_Branch,-1 * SRC.Entity_ID) AS [_Key_Branch_Transaction_Made]
			-- ,ISNULL(OrigBranch._Key_Branch,-1 * SRC.Entity_ID) AS [_Key_Branch_Ticket_Origin]
			,CASE
				WHEN OrigBranch.Branch_ID = TxnAt.Branch_ID
					THEN 'Own Store'
				ELSE 'Other Store'
			END AS Payment_for_Store_Type
			,CASE
				WHEN OrigBranch.Store_Group = TxnAt.Store_Group
					THEN 'Own Group'
				ELSE 'Other Group'
			END AS Payment_for_StoreGroup_Type
			,SRC.TransactionType
			,ROUND(
				CASE
					WHEN Payment_PaymentId is null AND SRC.TransactionType = 'RENEW' THEN SRC.Downloan_Amt 
					WHEN Payment_PaymentId is null AND SRC.TransactionType = 'REDEEM' THEN 0.0
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'RENEW'
						THEN
						CASE WHEN (SRC.InterestAmount+SRC.Downloan_Amt)<=Payment_Amt_Exclude_Charge THEN SRC.Downloan_Amt 
						ELSE SRC.Downloan_Amt*Payment_Amt_Exclude_Charge/(SRC.InterestAmount+SRC.Downloan_Amt) END
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'REDEEM' THEN 0.0
					ELSE 0.0 END 
				,2) AS Downloan_Amt
			,ROUND(
				CASE
					WHEN Payment_PaymentId is null THEN SRC.GrantAmount 
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'RENEW'
						THEN
						CASE WHEN (SRC.InterestAmount+SRC.Downloan_Amt)<=Payment_Amt_Exclude_Charge THEN SRC.GrantAmount 
						ELSE SRC.GrantAmount*Payment_Amt_Exclude_Charge/(SRC.InterestAmount+SRC.Downloan_Amt) END
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'REDEEM' 
						THEN
						CASE WHEN SRC.RedeemAmount<=Payment_Amt_Exclude_Charge THEN SRC.GrantAmount
						ELSE SRC.GrantAmount*Payment_Amt_Exclude_Charge/SRC.RedeemAmount END
					ELSE 0.0 END 
				,2) AS Principal_Amt
			,ROUND(
				CASE
					WHEN Payment_PaymentId is null THEN SRC.RedeemAmount 
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'RENEW'
						THEN
						CASE WHEN (SRC.InterestAmount+SRC.Downloan_Amt)<=Payment_Amt_Exclude_Charge THEN SRC.RedeemAmount 
						ELSE SRC.RedeemAmount*Payment_Amt_Exclude_Charge/(SRC.InterestAmount+SRC.Downloan_Amt) END
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'REDEEM'
						THEN
						CASE WHEN SRC.RedeemAmount<=Payment_Amt_Exclude_Charge THEN SRC.RedeemAmount
						ELSE Payment_Amt_Exclude_Charge END
					ELSE 0.0 END 
				,2) AS Redeem_Amt
			,ROUND(
				CASE	
					WHEN Payment_PaymentId is null THEN 0.0
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'RENEW' 
						THEN
						CASE WHEN (SRC.InterestAmount+SRC.Downloan_Amt)<Payment_Amt_Exclude_Charge THEN Payment_AdminCharge*(SRC.InterestAmount+SRC.Downloan_Amt)/Payment_Amt_Exclude_Charge
						ELSE Payment_AdminCharge END
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'REDEEM'
						THEN
						CASE WHEN SRC.RedeemAmount<Payment_Amt_Exclude_Charge THEN Payment_AdminCharge*SRC.RedeemAmount/Payment_Amt_Exclude_Charge
						ELSE Payment_AdminCharge END
					ELSE 0.0 END 
				,2) AS Payment_AdminCharge
			,ROUND(
				CASE	
					WHEN Payment_PaymentId is null THEN 0.0
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'RENEW' 
						THEN
						CASE WHEN (SRC.InterestAmount+SRC.Downloan_Amt)<Payment_Amt_Exclude_Charge THEN Payment_BankCharge*(SRC.InterestAmount+SRC.Downloan_Amt)/Payment_Amt_Exclude_Charge
						ELSE Payment_BankCharge END
					WHEN Payment_PaymentId is not null AND SRC.TransactionType = 'REDEEM'
						THEN
						CASE WHEN SRC.RedeemAmount<Payment_Amt_Exclude_Charge THEN Payment_BankCharge*SRC.RedeemAmount/Payment_Amt_Exclude_Charge
						ELSE Payment_BankCharge END
					ELSE 0.0 END
				,2) AS Payment_BankCharge
			,SRC.InterestAmount AS Total_Interest_Amt
			,SRC.Downloan_Amt AS Total_Downloan_Amt
			,SRC.GrantAmount AS Total_Principal_Amt
			,SRC.RedeemAmount AS Total_Redeem_Amt
			,cast(SRC.ReceiptId as string) AS Payment_ID
			,SRC.Payment_Reference AS Payment_Reference
			,SRC.Payment_Mode AS Payment_Mode
			,SRC.Payment_Merchant AS Payment_Merchant
	FROM (			
		SELECT SRC.*
		,Tic.GrantAmount AS New_Grant_Amt
		, CASE WHEN Tic.GrantAmount is not null and SRC.GrantAmount>coalesce(Tic.GrantAmount,0.0) THEN SRC.GrantAmount-coalesce(Tic.GrantAmount,0.0) ELSE 0.0 END AS Downloan_Amt
		,P.Reference AS Payment_Reference
		,Pt.ReportAlias AS Payment_Type
		,Pm.ReportAlias AS Payment_Mode
		,Pm.Merchant AS Payment_Merchant
		,P.PaymentId AS Payment_PaymentId
		,coalesce(P.Amount,0.0) AS Payment_Amt
	    ,COALESCE(P.Amount,0.0)-COALESCE(cast(json_extract_scalar(ExtraDetails,'$.Charges.AdminCharge' ) as float64),0.0) AS Payment_Amt_Exclude_Charge
    ,COALESCE(cast(json_extract_scalar(ExtraDetails,'$.Charges.AdminCharge' )as float64),0.0) AS Payment_AdminCharge
    ,COALESCE(cast(json_extract_scalar(ExtraDetails,'$.Charges.Charge' )as float64),0.0) AS Payment_BankCharge

			FROM (select 4 AS Entity_ID ,* from aspial-bq.dwh_pawn_gpdmsaustralia.gp_ticket_redeem_renew union all select 5 AS Entity_ID, * except(rowguid) from aspial-bq.dwh_pawn_gpdmshongkong.gp_ticket_redeem_renew union all select 6 AS Entity_ID, * from aspial-bq.dwh_pawn_gpdmsmalaysia.gp_ticket_redeem_renew)	SRC  
				          LEFT JOIN 
                  (select 4 AS Entity_ID ,* except(Dispose_BatchID)  from aspial-bq.dwh_pawn_gpdmsaustralia.gp_ticket_header union all select 5 AS Entity_ID ,* except(rowguid,Dispose_BatchID) from aspial-bq.dwh_pawn_gpdmshongkong.gp_ticket_header union all select 6 AS Entity_ID ,* from aspial-bq.dwh_pawn_gpdmsmalaysia.gp_ticket_header) Tic 
                  on SRC.Entity_ID=Tic.Entity_ID and SRC.RenewTicketNo=Tic.TicketNo
					LEFT JOIN 
          (select 4 AS Entity_ID ,* from aspial-bq.dwh_pawn_gpdmsaustralia.gp_payment union all select 5 AS Entity_ID ,* except(rowguid) from aspial-bq.dwh_pawn_gpdmshongkong.gp_payment union all select 6 AS Entity_ID ,* from aspial-bq.dwh_pawn_gpdmsmalaysia.gp_payment) P 
          on SRC.Entity_ID=P.Entity_ID AND SRC.ReceiptId=P.PaymentId
					LEFT JOIN 
          (select 4 AS Entity_ID ,* from aspial-bq.dwh_pawn_gpdmsaustralia.gp_payment_type union all select 5 AS Entity_ID ,* except(rowguid) from aspial-bq.dwh_pawn_gpdmshongkong.gp_payment_type union all select 6 AS Entity_ID ,* from aspial-bq.dwh_pawn_gpdmsmalaysia.gp_payment_type) Pt  
          on P.Entity_ID=Pt.Entity_ID AND P.PayTypeId=Pt.PayTypeId
					LEFT JOIN 
          (select 4 AS Entity_ID ,* from aspial-bq.dwh_pawn_gpdmsaustralia.gp_payment_mode union all select 5 AS Entity_ID ,* except(rowguid) from aspial-bq.dwh_pawn_gpdmshongkong.gp_payment_mode union all select 6 AS Entity_ID ,* from aspial-bq.dwh_pawn_gpdmsmalaysia.gp_payment_mode) Pm  
          on P.Entity_ID=Pm.Entity_ID AND P.PayModeId=Pm.PayModeId
      WHERE RTRIM(SRC.TransactionStatus) = 'VALID'
      #AND   CAST(CAST(SRC.transaction_date AS TIMESTAMP) AS DATE) >= Trans_From
      AND   SRC.CreatedBy NOT IN ('IKIOSK','IPAYMENT','MPAYMENT')
  ) SRC
  LEFT JOIN (select distinct Branch_ID, Store_Group from aspial-bq.datamart_pawn.Master_Branch where Entity_ID = 1 and Store_Group !='NA') 
 TxnAt  ON  SRC.RedeemBranchId = TxnAt.Branch_ID  
  LEFT JOIN (select distinct Branch_ID, Store_Group from  aspial-bq.datamart_pawn.Master_Branch where Entity_ID = 1 and Store_Group !='NA')
 OrigBranch  ON SRC.OriginBranchId = OrigBranch.Branch_ID 
)
