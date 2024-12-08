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

    -- Variables to accumulate total debits and credits for balancing
    v_debit_sum            NUMBER := 0;
    v_credit_sum           NUMBER := 0;

    -- Variables for account information
    v_account_balance      account.account_balance%TYPE;
    v_account_type_code    account.account_type_code%TYPE;
    v_default_trans_type   account_type.default_trans_type%TYPE;

BEGIN
    -- Open the transaction cursor
    OPEN c_transaction;
    LOOP
        FETCH c_transaction INTO v_transaction_no, v_transaction_date, v_description;        
        EXIT WHEN c_transaction%NOTFOUND;   

        -- Reset totals for the current transaction
        v_debit_sum := 0;
        v_credit_sum := 0;

        -- Insert into TRANSACTION_HISTORY
        INSERT INTO transaction_history (
            transaction_no,
            transaction_date,
            description
        ) VALUES (
            v_transaction_no,
            v_transaction_date,
            v_description
        );

        -- Open the detail cursor for the current transaction
        OPEN c_detail(v_transaction_no);
        LOOP
            FETCH c_detail INTO v_account_no, v_transaction_type, v_transaction_amount;
            EXIT WHEN c_detail%NOTFOUND;

            -- Retrieve account balance and account type code
            SELECT account_balance, account_type_code
            INTO v_account_balance, v_account_type_code
            FROM account
            WHERE account_no = v_account_no;

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
            
            -- Accumulate total debits and credits
            IF v_transaction_type = 'D' THEN
                v_debit_sum := v_debit_sum + v_transaction_amount;
            ELSE
                v_credit_sum := v_credit_sum + v_transaction_amount;
            END IF;

            -- Optional: Output for debugging
            DBMS_OUTPUT.PUT_LINE('Transaction No: ' || v_transaction_no ||
                                 ', Account No: ' || v_account_no ||
                                 ', Type: ' || v_transaction_type ||
                                 ', Amount: ' || v_transaction_amount);

        END LOOP;
        CLOSE c_detail;

        -- Delete processed transactions from NEW_TRANSACTIONS
        DELETE FROM new_transactions
        WHERE transaction_no = v_transaction_no;

        -- Optional: Check if debits equal credits for the transaction
        IF v_debit_sum != v_credit_sum THEN
            DBMS_OUTPUT.PUT_LINE('Transaction ' || v_transaction_no || ' is unbalanced: Debits = ' ||
                                 v_debit_sum || ', Credits = ' || v_credit_sum);
        END IF;

    END LOOP;
    CLOSE c_transaction;

    COMMIT;

END;
/
