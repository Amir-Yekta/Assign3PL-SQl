/* *****************************************************************
** Author:  Amir Hossein, Dalaso Hankebo, John Sugden-van Dyk, Mathieu Frenette
** Creation Date:  November 26, 2024                                   
** Description:  take transactions from a holding table named NEW_TRANSACTIONS and insert them
**          into the TRANSACTION_DETAIL and TRANSACTION_HISTORY tables. At the same time the program
**          will update the appropriate account balance in the ACCOUNT table. 
**          once a transaction is successfully processed, it will be removed from the holding table.  
**
** Modifications:
**
**
**
**
**                                            
*******************************************************************/
set SERVEROUTPUT ON
DECLARE
    -- Declaration of variables

    -- Outer cursor to get each unique transaction number
    CURSOR c_transaction IS 
        SELECT DISTINCT transaction_no, transaction_date, description
        FROM new_transactions;  

    -- Variable for the outer cursor
    v_transaction_no       new_transactions.transaction_no%TYPE;
    v_transaction_date     new_transactions.transaction_date%TYPE;
    v_description          new_transactions.description%TYPE;

    -- Inner cursor to get transaction details for a specific transaction number
    CURSOR c_detail (p_transaction_no new_transactions.transaction_no%TYPE) IS
        SELECT account_no, transaction_type, transaction_amount
        FROM new_transactions
        WHERE transaction_no = p_transaction_no;

    -- Variables for the inner cursor
    v_account_no           new_transactions.account_no%TYPE;
    v_transaction_type     new_transactions.transaction_type%TYPE;
    v_transaction_amount   new_transactions.transaction_amount%TYPE;

    -- constants for valid type's debit and credit 
    k_transaction_type_debit new_transactions.transaction_type%TYPE := 'D';
    k_transaction_type_credit new_transactions.transaction_type%TYPE := 'C';

    -- Variables to accumulate total debits and credits for balancing
    v_debit_sum            NUMBER := 0;
    v_credit_sum           NUMBER := 0;

    -- Variables for account information
    v_account_balance      account.account_balance%TYPE;
    v_account_type_code    account.account_type_code%TYPE;
    v_default_trans_type   account_type.default_trans_type%TYPE;

    -- Variable to check if there was any errors
    v_error_flag    BOOLEAN := FALSE;

     v_error_logged BOOLEAN := FALSE; --  first error logged // Kourosh 
     v_error_msg    VARCHAR2(200); -- // Kourosh 

     -- if not logged for the transaction the throw log error // Kouroush 
     PROCEDURE log_error
     (
        p_trans_no new_transactions.transaction_no%TYPE,
        p_trans_date DATE,
        p_desc VARCHAR2,
        p_msg VARCHAR2
    )
    
    IS
         
    BEGIN
        IF NOT v_error_logged THEN
            INSERT INTO wkis_error_log (transaction_no, transaction_date, description, error_msg)
            VALUES (p_trans_no, p_trans_date, p_desc, p_msg);
            v_error_logged := TRUE;

        END IF;
        DBMS_OUTPUT.PUT_LINE('Error logged for transaction '||p_trans_no||': '||p_msg);

    END log_error;

    -- if/when you are building the error log table this is it incase you need it. 

    -- this is the error log table
    -- Name                                      Null?    Type
    -- ----------------------------------------- -------- ----------------------------
    -- TRANSACTION_NO                                     NUMBER
    -- TRANSACTION_DATE                                   DATE
    -- DESCRIPTION                                        VARCHAR2(100)
    -- ERROR_MSG                                          VARCHAR2(200)


BEGIN

    DBMS_OUTPUT.PUT_LINE('Transaction processing...'); -- // declare for debug and error // Kourosh 
    -- Open the transaction cursor
    OPEN c_transaction;
    LOOP
        FETCH c_transaction INTO v_transaction_no, v_transaction_date, v_description;        
        EXIT WHEN c_transaction%NOTFOUND;   

        -- reset error flag for the transaction 
        v_error_flag := FALSE;
        v_error_logged := FALSE; 
        v_error_msg := NULL;
        v_debit_sum := 0;
        v_credit_sum := 0;


        -- check for missing transaction numbers 
        IF v_transaction_no IS NULL THEN
            -- need to properly insert into the error log table,
            v_error_flag := TRUE;
            -- temp output to showcase error
            DBMS_OUTPUT.PUT_LINE('Transaction No: is missing');

            -- Log error (if not already logged)
            log_error(NULL, v_transaction_date, v_description, v_error_msg);
            -- CONTINUE;

        END IF;


-- If no error then continue

IF NOT v_error_flag THEN

    OPEN c_detail(v_transaction_no);

    LOOP
        FETCH c_detail INTO v_account_no, v_transaction_type, v_transaction_amount;

        EXIT WHEN c_detail%NOTFOUND;

        -- Validate line data

        IF NOT v_error_flag THEN

            -- Look for transaction type

            IF v_transaction_type NOT IN (k_transaction_type_credit, k_transaction_type_debit) THEN

                v_error_flag := TRUE;
                v_error_msg := 'ERROR: Invalid transaction type '||v_transaction_type||' for transaction '||v_transaction_no||'.';
                log_error(v_transaction_no, v_transaction_date, v_description, v_error_msg);

            END IF;

            -- if negative amount
            IF NOT v_error_flag AND v_transaction_amount < 0 THEN

                v_error_flag := TRUE;
                v_error_msg := 'ERROR: Negative transaction amount '||v_transaction_amount||' for transaction '||v_transaction_no||'.';
                log_error(v_transaction_no, v_transaction_date, v_description, v_error_msg);

            END IF;

            -- Check account existence
            
            IF NOT v_error_flag THEN

                BEGIN

                    SELECT account_balance, account_type_code
                      INTO v_account_balance, v_account_type_code
                      FROM account WHERE account_no = v_account_no;

                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_error_flag := TRUE;
                        v_error_msg := 'ERROR: Invalid account number '||v_account_no||' for transaction '||v_transaction_no||'.';
                        log_error(v_transaction_no, v_transaction_date, v_description, v_error_msg);
                END;

            END IF;

        END IF;

        
        -- Note: next steps will involve calculations and further processing.
        -- compute the debit and credit and update the account balances.
        -- Also make sure to properly insert into TRANSACTION_DETAIL and handle the deletion of those transactions that have been processed from NEW_TRANSACTIONS where applicable.

    END LOOP;

    CLOSE c_detail;

END IF;


        -- Insert into TRANSACTION_HISTORY if no errors.
        IF NOT v_error_flag THEN
            INSERT INTO transaction_history (
                transaction_no,
                transaction_date,
                description
            ) VALUES (
                v_transaction_no,
                v_transaction_date,
                v_description
            );
        END IF;

        -- Open the detail cursor for the current transaction
        OPEN c_detail(v_transaction_no);
        LOOP
            FETCH c_detail INTO v_account_no, v_transaction_type, v_transaction_amount;
            EXIT WHEN c_detail%NOTFOUND;

            -- check for invalid transaction types
            IF v_transaction_type NOT IN (k_transaction_type_credit, k_transaction_type_debit) THEN 

                -- insert error into wkis_error_log

                --temp error output 
                DBMS_OUTPUT.PUT_LINE('Error: Invalid transaction type for: ' || v_transaction_no);

                v_error_flag := TRUE;
            END IF;

            -- embedded anonymous block for error handling of Invalid account numbers 
            BEGIN
                -- Retrieve account balance and account type code
                SELECT account_balance, account_type_code
                INTO v_account_balance, v_account_type_code
                FROM account
                WHERE account_no = v_account_no;

                EXCEPTION
                    WHEN NO_DATA_FOUND THEN

                    v_error_flag := TRUE;

                    -- insert error into wkis_error_log

                    --temp error output 
                    DBMS_OUTPUT.PUT_LINE('Error: Invalid account number for transaction no: ' || v_transaction_no);
                    CONTINUE;
            END;

            -- Retrieve default transaction type from account_type
            SELECT default_trans_type
            INTO v_default_trans_type
            FROM account_type
            WHERE account_type_code = v_account_type_code;

            -- Update account balance based on transaction type
            IF v_transaction_type = v_default_trans_type THEN
                -- Transaction type matches default; increase the balance
                v_account_balance := v_account_balance + v_transaction_amount;
            ELSE
                -- Transaction type is opposite; decrease the balance
                v_account_balance := v_account_balance - v_transaction_amount;
            END IF;

            -- if no errors then continue with updating
            IF NOT v_error_flag THEN
                -- Update the ACCOUNT table with the new balance
                UPDATE account
                SET account_balance = v_account_balance
                WHERE account_no = v_account_no;

                -- Insert into TRANSACTION_DETAIL
                INSERT INTO transaction_detail (
                    account_no,
                    transaction_no,
                    transaction_type,
                    transaction_amount
                ) VALUES (
                    v_account_no,
                    v_transaction_no,
                    v_transaction_type,
                    v_transaction_amount
                );
            END IF;
            
            -- Accumulate total debits and credits
            IF v_transaction_type = k_transaction_type_debit THEN
                v_debit_sum := v_debit_sum + v_transaction_amount;
            ELSE
                v_credit_sum := v_credit_sum + v_transaction_amount;
            END IF;

            -- Output for debugging
            DBMS_OUTPUT.PUT_LINE('Transaction No: ' || v_transaction_no ||
                                 ', Account No: ' || v_account_no ||
                                 ', Type: ' || v_transaction_type ||
                                 ', Amount: ' || v_transaction_amount);

        END LOOP;
        CLOSE c_detail;

        -- Delete processed transactions from NEW_TRANSACTIONS
        DELETE FROM new_transactions
        WHERE transaction_no = v_transaction_no;

        -- change this to an error (handle it etc): Check if debits equal credits for the transaction
        IF v_debit_sum != v_credit_sum THEN
            DBMS_OUTPUT.PUT_LINE('Transaction ' || v_transaction_no || ' is unbalanced: Debits = ' ||
                                 v_debit_sum || ', Credits = ' || v_credit_sum);
        END IF;

    END LOOP;
    CLOSE c_transaction;

    COMMIT;

END;
/
