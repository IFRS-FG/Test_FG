CREATE OR REPLACE PROCEDURE `fg_ifrs_datamart_metadata.SplitStringAndFormat`()
BEGIN

    DECLARE output_string STRING;
    DECLARE output_string2 STRING;
    DECLARE formatted_string STRING;
    DECLARE v_where_cond STRING;
    DECLARE counter INT64;

    SET output_string = '';
    SET output_string2 = '';
    SET counter = 1;
    
    -- Split the input string by comma and iterate over the values
    FOR i IN (SELECT value FROM UNNEST(SPLIT('ANNEX_7_CODE,POSTING_DIRECTION,SUN_GL_CODE', ',')) AS value) DO
        -- Format each value as value=value and concatenate them
        SET formatted_string = CONCAT('s.',i.value, ' = t.' , i.value);
        SET v_where_cond = CONCAT('t.',i.value, ' IS NULL ');

        SET output_string = CONCAT(output_string, formatted_string);
        SET output_string2 = CONCAT(output_string2, v_where_cond);
        
        -- Append ' AND ' between values, except for the last one
        IF counter < ARRAY_LENGTH(SPLIT('ANNEX_7_CODE,POSTING_DIRECTION,SUN_GL_CODE', ',')) THEN
            SET output_string = CONCAT(output_string, ' AND ');
            SET output_string2 = CONCAT(output_string2, ' AND ');
        END IF;
        
        SET counter = counter + 1;
    END FOR;
    
    -- Output the formatted string
    SELECT output_string AS formatted_output;
    SELECT CONCAT(' (', output_string2, ') ') AS v_where_condition;
END;