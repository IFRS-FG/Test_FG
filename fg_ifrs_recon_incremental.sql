CREATE OR REPLACE PROCEDURE `fg-ifrs-dev.fg_ifrs_datamart_metadata.sp_ifrs_recon_incremental_load`(IN p_job_id STRING)
BEGIN
	DECLARE v_source_system,v_transformation,v_source_table_name,v_target_table_name,v_primary_key,v_md5columns,v_filter_clause STRING;
	DECLARE v_audit_id INT64;
	DECLARE v_insert_query, v_delete_query,v_target_column_list,v_temp_target_table,v_temp_insert_query,v_drop_query STRING;
	DECLARE v_current_month,v_current_year,v_quarter,v_execution_frequency STRING;

		--Calculating the next Audit ID for the respective job id
		SET v_audit_id = (SELECT COALESCE(MAX(AUDIT_ID),0) + 1 
							FROM `fg-ifrs-dev.fg_ifrs_datamart_metadata.AUDIT_TABLE` WHERE JOB_ID=p_job_id);

		CALL `fg-ifrs-dev.fg_ifrs_datamart_metadata.sp_ifrs_logging`(v_audit_id,p_job_id,'Load Initiated');

					-- Create Audit entry in AUDIT TABLE
			INSERT INTO `fg-ifrs-dev.fg_ifrs_datamart_metadata.AUDIT_TABLE` 
			(AUDIT_ID, JOB_ID, PROCEDURE_NAME, LOAD_STATUS, START_DATETIME) 
			VALUES
			(v_audit_id , p_job_id ,'fg-ifrs-dev.sp_ifrs_incremental_load','Started', CURRENT_DATETIME());


		-- Set values from the config table
		SET (v_source_system, v_source_table_name, v_target_table_name, v_primary_key, v_md5columns, v_transformation, v_filter_clause,v_execution_frequency) = 
			( SELECT AS STRUCT  source_system, source_table_name, target_table_name, primary_key, md5columns, transformation, filter_clause,EXECUTION_FREQUERY 
				FROM `fg-ifrs-dev.fg_ifrs_datamart_metadata.CONFIG_TABLE` 
				WHERE JOB_ID = p_job_id
			);


	   -- Replace parameter values in transformation Query
			FOR i IN
					(SELECT 
						PARAMETER_COLUMN AS PC,
						PARAMETER_VALUE AS PV 
					FROM `fg-ifrs-dev.fg_ifrs_datamart_metadata.PARAMETER_TABLE` 
					WHERE (JOB_ID ='ALL' OR JOB_ID = p_job_id) AND ISACTIVE = 'Y')
			DO
			SET v_transformation = REPLACE(v_transformation,i.PC,i.PV);
			END FOR;
			
			-- Create temp table for INCREMENTAL load from transformation Query
			SET v_temp_target_table = CONCAT(v_target_table_name,FORMAT_TIMESTAMP('%Y_%m_%d',CURRENT_TIMESTAMP()));

			SET v_temp_insert_query = CONCAT('CREATE OR REPLACE TABLE `',v_temp_target_table,'` AS ',v_transformation||' '||CASE WHEN v_filter_clause IS NULL THEN '' ELSE v_filter_clause END);

			EXECUTE IMMEDIATE v_temp_insert_query;

			-- Temp Table Created
			CALL `fg-ifrs-dev.fg_ifrs_datamart_metadata.sp_ifrs_logging`(v_audit_id, p_job_id,CONCAT ('Temp Table ', v_temp_target_table,' Created'));

			CALL `fg-ifrs-dev.fg_ifrs_datamart_metadata.sp_ifrs_logging`(v_audit_id,p_job_id,CONCAT('New data from ',v_source_table_name,' has been copied into',v_temp_target_table));

			--Retrieving Current Month and Current Year Values from Parameter Table

			SET v_current_month = (SELECT parameter_value from `fg-ifrs-dev.fg_ifrs_datamart_metadata.PARAMETER_TABLE` where job_id in ('ALL',p_job_id) and parameter_column = '$MONTH' AND ISACTIVE = 'Y');

			SET v_current_year = (SELECT parameter_value from `fg-ifrs-dev.fg_ifrs_datamart_metadata.PARAMETER_TABLE` where job_id in ('ALL',p_job_id) and parameter_column = '$YEAR' AND ISACTIVE = 'Y');

			SET v_quarter = (SELECT parameter_value from `fg-ifrs-dev.fg_ifrs_datamart_metadata.PARAMETER_TABLE` where job_id in ('ALL',p_job_id) and parameter_column = '$QUARTER' AND ISACTIVE = 'Y');


			--Delete existing records with Current Month/Year if any from target table
			IF v_execution_frequency = 'QUARTERLY' THEN
				SET v_delete_query = CONCAT ('DELETE FROM ',v_target_table_name,' WHERE YEAR = ',v_current_year,' AND MONTH IN (',v_quarter,'*3-1 ,',v_quarter,'*3-2 ,',v_quarter,'*3 )', ' AND TRIM(SOURCE_TABLE_NAME) = \'',v_source_table_name,'\'');
			ELSE
				SET v_delete_query = CONCAT ('DELETE FROM ',v_target_table_name,' WHERE YEAR = ',v_current_year,' AND MONTH = ',v_current_month, ' AND TRIM(SOURCE_TABLE_NAME) = \'',v_source_table_name,'\'');
		  	-- confirm year and month names --CAST(BATCACTYR AS STRING) || CAST(BATACTMN AS STRING)=
			END IF;
			EXECUTE IMMEDIATE v_delete_query;
			
			
			SET v_target_column_list = CONCAT(v_primary_key,',',v_md5columns);

			SET v_insert_query = CONCAT('INSERT INTO ',v_target_table_name, '( ',REGEXP_REPLACE(v_target_column_list, r'[^a-zA-Z0-9_,]',''), ' ,INSERT_TIMESTAMP,AUDIT_ID) SELECT ',' ',REGEXP_REPLACE(v_target_column_list, r'[^a-zA-Z0-9_,]',''), ', CURRENT_TIMESTAMP() ,',v_audit_id,' FROM ',v_temp_target_table);

			select v_insert_query;

			EXECUTE IMMEDIATE	v_insert_query;
	
			CALL `fg-ifrs-dev.fg_ifrs_datamart_metadata.sp_ifrs_logging`(v_audit_id,p_job_id,CONCAT(@@row_count, 'rows inserted into ',v_target_table_name,' for the period ',CONCAT(v_current_month,'-',v_current_year)));

			--Dropping temporary table

			SET v_drop_query =  CONCAT('DROP TABLE ',v_temp_target_table);

			EXECUTE IMMEDIATE v_drop_query;

			CALL `fg-ifrs-dev.fg_ifrs_datamart_metadata.sp_ifrs_logging`(v_audit_id,p_job_id,CONCAT('Temporary table ',V_TEMP_TARGET_TABLE, ' is dropped.'));

			CALL `fg-ifrs-dev.fg_ifrs_datamart_metadata.sp_ifrs_dq_check`(p_job_id,v_audit_id);

					-- Update Audit Error message
									UPDATE `fg-ifrs-dev.fg_ifrs_datamart_metadata.AUDIT_TABLE` 
										SET END_DATETIME = CURRENT_DATETIME(),
											LOAD_STATUS = 'Success'
										WHERE AUDIT_ID = v_audit_id AND JOB_ID = p_job_id;

			CALL `fg-ifrs-dev.fg_ifrs_datamart_metadata.sp_ifrs_logging`(v_audit_id,p_job_id,' Load Completed');

			EXCEPTION WHEN ERROR THEN
			-- Update Audit Error message
			UPDATE `fg-ifrs-dev.fg_ifrs_datamart_metadata.AUDIT_TABLE` 
			SET END_DATETIME = CURRENT_DATETIME(),
				LOAD_STATUS = 'Failed',
				ERROR_MSG = (SELECT	@@error.message)
			WHERE AUDIT_ID = v_audit_id AND JOB_ID = p_job_id;

			CALL `fg-ifrs-dev.fg_ifrs_datamart_metadata.sp_ifrs_logging`(v_audit_id,p_job_id,'Error in sp_ifrs_incremental_load');

END;