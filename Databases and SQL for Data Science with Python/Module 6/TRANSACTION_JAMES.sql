DELIMITER //

CREATE PROCEDURE TRANSACTION_JAMES()

BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    UPDATE BankAccounts
    SET Balance = Balance-1200
    WHERE AccountName = 'James';

    UPDATE BankAccounts
    SET Balance = Balance+1200
    WHERE AccountName = 'Shoe Shop';

    UPDATE ShoeShop
    SET Stock = Stock-4
    WHERE Product = 'Trainers';

    UPDATE BankAccounts
    SET Balance = Balance-150
    WHERE AccountName = 'James';

    COMMIT;

END //

DELIMITER ; 