EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS -- Code als Skript speichern, wenn Code oft verwendet wird
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; -- Setzt die Variablen für Start- und Endzeit
	BEGIN TRY -- Wird ausgeführt und wenn ein Fehler auftritt, kommt es zum CATCH
		SET @batch_start_time = GETDATE();
		PRINT '###############################################################################'; -- Bessere Ansicht, damit klar wird, was durch das Skript geladen wird
		PRINT 'Loading Bronze Layer';
		PRINT '###############################################################################';

		PRINT '-------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------------------------------------';

		SET @start_time = GETDATE(); -- Setzt Datum und Uhrzeit, wenn begonnen wird, die erste Tabelle geladen wird
		PRINT '--> Truncate Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info; -- Tabelle leeren, damit die .csv nicht doppelt eingefügt wird ("Aktualisierung der Tabelle")

		PRINT '--> Inserting Data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info -- Full Load
		FROM 'C:\Users\OlafK\Desktop\Dateien\Football\OutdoorEscape\GitHub\Repository\Data-Warehouse\dataset\source_crm\cust_info.csv' 
		WITH( 
			FIRSTROW = 2,		     -- Erste Reihe wird herausgenommen, weil sie nur die Header beinhaltet 
			FIELDTERMINATOR = ',',	 -- Trennzeichen, je nach Datei unterschiedlich(, oder ; etc.) 
			TABLOCK					 -- sperrt die Tabelle (bessere Performance bei Bulk Insert) 
		);
		SET @end_time = GETDATE(); -- Setzt Datum und Uhrzeit, wenn die erste Tabelle fertig geladen wurde
		PRINT '--> LOAD DURATION: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; -- Zeigt an, wie lange das Laden der Tabelle gedauert hat
		PRINT '----------------------------';

		-- SELECT * FROM bronze.crm_cust_info -- überprüfen, ob in jeder Spalte Daten stehen und ob die Daten auch in die richtigen Spalten eingefügt wurden
		-- SELECT COUNT(*) FROM bronze.crm_cust_info -- überprüfen, ob alle Zeilen eingefügt wurden

		SET @start_time = GETDATE();
		PRINT '--> Truncate Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info; 

		PRINT '--> Inserting Data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\OlafK\Desktop\Dateien\Football\OutdoorEscape\GitHub\Repository\Data-Warehouse\dataset\source_crm\prd_info.csv' 
		WITH( 
			FIRSTROW = 2,		     
			FIELDTERMINATOR = ',',	 
			TABLOCK					 
		);
		SET @end_time = GETDATE();
		PRINT '--> LOAD DURATION: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; -- Zeigt an, wie lange das Laden der Tabelle gedauert hat
		PRINT '----------------------------';

		SET @start_time = GETDATE();
		PRINT '--> Truncate Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '--> Inserting Data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\OlafK\Desktop\Dateien\Football\OutdoorEscape\GitHub\Repository\Data-Warehouse\dataset\source_crm\sales_details.csv' 
		WITH( 
			FIRSTROW = 2,		     
			FIELDTERMINATOR = ',',	 
			TABLOCK					 
		);
		SET @end_time = GETDATE();
		PRINT '--> LOAD DURATION: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; -- Zeigt an, wie lange das Laden der Tabelle gedauert hat
		PRINT '----------------------------';

		PRINT '-------------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '--> Truncate Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '--> Inserting Data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\OlafK\Desktop\Dateien\Football\OutdoorEscape\GitHub\Repository\Data-Warehouse\dataset\source_erp\CUST_AZ12.csv' 
		WITH( 
			FIRSTROW = 2,		     
			FIELDTERMINATOR = ',',	 
			TABLOCK					 
		);
		SET @end_time = GETDATE();
		PRINT '--> LOAD DURATION: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; -- Zeigt an, wie lange das Laden der Tabelle gedauert hat
		PRINT '----------------------------';

		SET @start_time = GETDATE();
		PRINT '--> Truncate Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '--> Inserting Data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\OlafK\Desktop\Dateien\Football\OutdoorEscape\GitHub\Repository\Data-Warehouse\dataset\source_erp\LOC_A101.csv' 
		WITH( 
			FIRSTROW = 2,		     
			FIELDTERMINATOR = ',',	 
			TABLOCK					 
		);
		SET @end_time = GETDATE();
		PRINT '--> LOAD DURATION: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; -- Zeigt an, wie lange das Laden der Tabelle gedauert hat
		PRINT '----------------------------';

		SET @start_time = GETDATE();
		PRINT '--> Truncate Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '--> Inserting Data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\OlafK\Desktop\Dateien\Football\OutdoorEscape\GitHub\Repository\Data-Warehouse\dataset\source_erp\PX_CAT_G1V2.csv' 
		WITH( 
			FIRSTROW = 2,		     
			FIELDTERMINATOR = ',',	 
			TABLOCK					 
		);
		SET @end_time = GETDATE();
		PRINT '--> LOAD DURATION: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; -- Zeigt an, wie lange das Laden der Tabelle gedauert hat
		PRINT '----------------------------';

		SET @batch_end_time = GETDATE();
		PRINT 'Loading Bronze Layer is Completed';	
		PRINT '	--> TOTAL LOAD DURATION : ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '----------------------------';

	END TRY
	BEGIN CATCH														 -- Wenn ein Fehler auftritt, wird dieser Code ausgeführt
		PRINT '=======================================================================';
		PRINT 'ERROR DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();					 -- Gibt die Errornachricht aus
		PRINT 'ERROR MESSAGE: ' + CAST (ERROR_NUMBER() AS NVARCHAR); -- Da Error_Number eine Zahl ist, aber Error Message ein Text, muss Error_Number als Text umgewandelt werden
		PRINT 'ERROR MESSAGE: ' + CAST (ERROR_STATE() AS NVARCHAR);  -- Gibt den Fehlercode des Fehlers zurück.
		PRINT '=======================================================================';
	END CATCH
END
