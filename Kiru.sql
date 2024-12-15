DECLARE
    -- Constants for transaction types
    c_credit CONSTANT CHAR(1) := 'C';
    c_debit CONSTANT CHAR(1) := 'D';

    -- Outer cursor to get each unique transaction number
    CURSOR c_transaction IS 
        SELECT DISTINCT transaction_no, transaction_date, description
        FROM new_transactions;

    -- Variables for outer cursor
    v_transaction_no       new_transactions.transaction_no%TYPE;
    v_transaction_date     new_transactions.transaction_date%TYPE;
    v_description          new_transactions.description%TYPE;

    -- Inner cursor to get transaction details for a specific transaction number
    CURSOR c_detail (p_transaction_no new_transactions.transaction_no%TYPE) IS
        SELECT account_no, transaction_type, transaction_amount
        FROM new_transactions
        WHERE transaction_no = p_transaction_no;

    -- Variables for inner cursor
    v_account_no           new_transactions.account_no%TYPE;
    v_transaction_type     new_transactions.transaction_type%TYPE;
    v_transaction_amount   new_transactions.transaction_amount%TYPE;

    -- Variables for totals
    v_debit_sum            NUMBER := 0;
    v_credit_sum           NUMBER := 0;

    -- Variables for account data
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

        -- Exception handling for the current transaction
        BEGIN
            -- **Handle Missing transaction number (NULL transaction_no)**
            IF v_transaction_no IS NULL THEN
                INSERT INTO wkis_error_log (
                    transaction_no, transaction_date, description, error_msg
                ) VALUES (
                    NULL, v_transaction_date, v_description, 
                    'Error: Missing transaction number'
                );
                CONTINUE;
            END IF;

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

            -- Open the detail cursor for this transaction
            OPEN c_detail(v_transaction_no);

            LOOP
                FETCH c_detail INTO v_account_no, v_transaction_type, v_transaction_amount;
                EXIT WHEN c_detail%NOTFOUND;

                -- Retrieve account balance and account type code
                BEGIN
                    SELECT account_balance, account_type_code
                    INTO v_account_balance, v_account_type_code
                    FROM account
                    WHERE account_no = v_account_no;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        INSERT INTO wkis_error_log (
                            transaction_no, transaction_date, description, error_msg
                        ) VALUES (
                            v_transaction_no, v_transaction_date, v_description,
                            'Error: Invalid account number ' || v_account_no
                        );
                        ROLLBACK;
                        CLOSE c_detail;
                        CONTINUE;
                END;

                -- **Handle Negative transaction amount**
                IF v_transaction_amount < 0 THEN
                    INSERT INTO wkis_error_log (
                        transaction_no, transaction_date, description, error_msg
                    ) VALUES (
                        v_transaction_no, v_transaction_date, v_description,
                        'Error: Negative transaction amount ' || v_transaction_amount
                    );
                    ROLLBACK;
                    CLOSE c_detail;
                    CONTINUE;
                END IF;

                -- **Handle Invalid transaction type**
                IF v_transaction_type NOT IN (c_credit, c_debit) THEN
                    INSERT INTO wkis_error_log (
                        transaction_no, transaction_date, description, error_msg
                    ) VALUES (
                        v_transaction_no, v_transaction_date, v_description,
                        'Error: Invalid transaction type ' || v_transaction_type
                    );
                    ROLLBACK;
                    CLOSE c_detail;
                    CONTINUE;
                END IF;

                -- Retrieve default transaction type from account_type
                SELECT default_trans_type
                INTO v_default_trans_type
                FROM account_type
                WHERE account_type_code = v_account_type_code;

                -- Update account balance based on transaction type
                IF v_transaction_type = v_default_trans_type THEN
                    v_account_balance := v_account_balance + v_transaction_amount;
                ELSE
                    v_account_balance := v_account_balance - v_transaction_amount;
                END IF;

                -- Update the ACCOUNT table
                UPDATE account
                SET account_balance = v_account_balance
                WHERE account_no = v_account_no;

                -- Insert into TRANSACTION_DETAIL
                INSERT INTO transaction_detail (
                    account_no, transaction_no, transaction_type, transaction_amount
                ) VALUES (
                    v_account_no, v_transaction_no, v_transaction_type, v_transaction_amount
                );

                -- Accumulate totals
                IF v_transaction_type = c_debit THEN
                    v_debit_sum := v_debit_sum + v_transaction_amount;
                ELSE
                    v_credit_sum := v_credit_sum + v_transaction_amount;
                END IF;
            END LOOP;

            CLOSE c_detail;

            -- Check if debits and credits are balanced
            IF v_debit_sum != v_credit_sum THEN
                INSERT INTO wkis_error_log (
                    transaction_no, transaction_date, description, error_msg
                ) VALUES (
                    v_transaction_no, v_transaction_date, v_description,
                    'Error: Debits and/or credits do not balance'
                );
                ROLLBACK;
                CONTINUE;
            END IF;

            -- Delete processed transaction
            DELETE FROM new_transactions WHERE transaction_no = v_transaction_no;

        EXCEPTION
            -- Handle unanticipated errors
            WHEN OTHERS THEN
                INSERT INTO wkis_error_log (
                    transaction_no, transaction_date, description, error_msg
                ) VALUES (
                    v_transaction_no, v_transaction_date, v_description,
                    'Error: ' || SQLERRM
                );
                ROLLBACK;
        END;
    END LOOP;

    CLOSE c_transaction;

    -- Commit the transaction
    COMMIT;

END;
/
