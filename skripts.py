import pandas as pd

sql_delete = """
drop table if exists xsdfiles;
drop table if exists tabletags;
drop table if exists arcs;
drop table if exists aspectnodes;
drop table if exists catalog;
drop table if exists elements;
drop table if exists elements_labels;
drop table if exists entrypoints;
drop table if exists labels;
drop table if exists linkbaserefs;
drop table if exists locators;
drop table if exists messages;
drop table if exists preconditions;
drop table if exists preferred_labels;
drop table if exists rend_edimensions;
drop table if exists rend_edmembers;
drop table if exists rolerefs;
drop table if exists roletypes;
drop table if exists rulenodes;
drop table if exists rulenodes_c;
drop table if exists rulenodes_e;
drop table if exists rulenodes_p;
drop table if exists rulesets;
drop table if exists tableparts;
drop table if exists tables;
drop table if exists tableschemas;
drop table if exists taxpackage;
drop table if exists va_orfilters;
drop table if exists va_aspectcovers;
drop table if exists va_assertions;
drop table if exists va_assertionsets;
drop table if exists va_concepts;
drop table if exists va_edimensions;
drop table if exists va_edmembers;
drop table if exists va_factvars;
drop table if exists va_generals;
drop table if exists va_tdimensions;
drop table if exists rend_conceptrelnodes;
drop table if exists roles_table_definition;
"""
sql_create_functions = """

CREATE OR REPLACE FUNCTION public.get_latin_element_text(
	elements text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    i integer;
    result text;
BEGIN
    FOR i IN 1..length(elements) LOOP
        IF substr(elements, i, 1) ~ '^[A-Za-z]$' THEN
            result := result||'|> '||substr(elements, i, 1)||' <|';
		else
			result :=result||substr(elements, i, 1);
        END IF;
    END LOOP;

    RETURN result;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.get_latin_element(
	elements text)
    RETURNS integer[]
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    i integer;
    result integer[];
BEGIN
    FOR i IN 1..length(elements) LOOP
        IF substr(elements, i, 1) ~ '^[A-Za-z]$' THEN
            result := array_append(result, i);
        END IF;
    END LOOP;

    RETURN result;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.arr1_in_arr2_hard(
	array1 text[],
	array2 text[])
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
   	elem text;
	check_ int :=0;
BEGIN
	if array2 is null
	then
	RETURN 0;
	end if;
	
	
	if array1=array2 then
		return 1;
	end if;
		
	FOREACH elem IN ARRAY array1
	loop
		if elem = ANY (select xx from unnest(array2) as xx) then
			check_=1;
		else
			return 0;
		end if;
	end loop;
	if check_=1 then
		return 1;
	else 
		return 0;
	end if;
	
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.arr1_in_arr2_light(
	array1 text[],
	array2 text[])
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
   	elem text;
	check_ int :=0;
BEGIN
	if array2 is null
	then
	RETURN 0;
	end if;
	
	
	if array1=array2 then
		return 1;
	end if;
		
	FOREACH elem IN ARRAY array1
	loop
		if lower( replace(delete_space_and_tab(elem), E'\u00a0', ' ') )= ANY (select lower( replace(delete_space_and_tab(xx), E'\u00a0', ' ') ) from unnest(array2) as xx) then
			check_=1;
		else
			return 0;
		end if;
	end loop;
	if check_=1 then
		return 1;
	else 
		return 0;
	end if;
	
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.compare_arrays_datecontrol(
	arr1 text[],
	arr2 text[],
	arr3 text[])
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    compare_elements text[];
    subarrays text[];
	subarr text;
	check_element text;
	check_ int := 0;
BEGIN
	--RAISE NOTICE 'arr1  %', arr1;

    IF arr2 IS NULL or arr1 = '{}' or array_length(arr3,1)=1 THEN
        RETURN 0;
    END IF;
	
	if arr3 !='{}' then
	foreach subarr in array arr1
	loop
		if split_part(subarr,'#',1) = ANY(arr3) then
		else 
		arr1=array_remove(arr1,subarr);
		end if;
	end loop;
	end if;
	
	foreach subarr in array arr2
		loop
			subarrays = string_to_array(subarr, ';');
			--RAISE NOTICE 'subarrays  %', subarrays;
			check_=compare_two_arrays(arr1,subarrays);
			--RAISE NOTICE 'subarrays  %', check_;
			if check_=1 then
				return 1;
			end if;
		end loop;
	return 0;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.remove_elements_from_array_datecontrol(
	arr1 text[],
	arr2 text[])
    RETURNS text[]
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE    result text[];
xx text;
subarr text;
BEGIN
    if arr1 is not null and arr2 is not null then
		FOREACH subarr IN ARRAY arr2
		loop
			foreach xx in array string_to_array(subarr,';')
			LOOP  
 				if split_part(xx,'#',1) = ANY (select split_part(yy,'#',1) from unnest(arr1) as yy) then
--  				if xx = ANY (arr1) then
					result = result||split_part(xx,'#',1);
				end if;
			END LOOP;    
		end loop;
		RETURN array_unique(result);
	else 
	return(arr1);
	end if;
END;
$BODY$;



CREATE OR REPLACE FUNCTION public.compare_arrays2(
	array1 text[],
	array2 text[])
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    comparison_elements text[];
BEGIN
	if array2 is null
	then
	RETURN 1;
	end if;
		
    -- Находим "элементы для сравнения", которые есть в массиве 1
    SELECT array_agg(element) INTO comparison_elements
    FROM unnest(array1) as element
    WHERE element = ANY (array2);
	
-- 	raise notice '%',comparison_elements;
-- 	raise notice '%',array_length(comparison_elements,1);
    
	if array_length(comparison_elements,1)>=1 then
        RETURN 1;
    ELSE
        RETURN 0;
    END IF;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.array_unique(
	arr anyarray)
    RETURNS text[]
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
select array( select x from (select distinct unnest($1) as x) z where x is not null  order by 1)
$BODY$;

CREATE OR REPLACE FUNCTION public.check_similarity(
	str1 text,
	arr_str text[])
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    found boolean := false;
BEGIN
    FOR i IN 1..array_length(arr_str, 1) LOOP
        IF  arr_str[i] similar to str1||'\D%' or str1=arr_str[i] THEN
            found := true;
            EXIT;
        END IF;
    END LOOP;

    IF found THEN
        RETURN 1;
    ELSE
        RETURN 0;
    END IF;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.compare_arrays(
	arr1 text[],
	arr2 text[])
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    compare_elements text[];
    subarrays text[];
	subarr text;
	check_element text;
	check_ int :=1;
BEGIN
    -- Если arr2 равен NULL, возвращаем 0
    IF arr2 IS NULL THEN
        RETURN 0;
    END IF;

    -- Разбиваем элементы arr2 на подмассивы
    FOREACH subarr IN ARRAY arr2
    LOOP
        subarrays := array( select split_part(x,'#',1) from unnest(string_to_array(subarr, ';')) as x )  ;
 		RAISE NOTICE 'Notice message  %', subarrays;
    

		-- Убираем дубликаты и пустые строки
		subarrays := array_remove(subarrays, NULL);
		subarrays := array_remove(subarrays, '');
-- 		RAISE NOTICE 'Notice message  %', subarrays;
	
	

		-- Находим элементы для сравнения (которые есть в arr1 и в subarrays)
		SELECT array_agg(DISTINCT elem)
		INTO compare_elements
		FROM unnest(arr1) AS elem
		WHERE split_part(elem,'#',1) = ANY(subarrays);
		if compare_elements is null then
			return 0;
		end if;

		FOREACH subarr IN ARRAY arr2
			loop
				FOREACH check_element in array compare_elements
					loop
						if check_element = ANY (string_to_array(subarr, ';')) then
						else
						check_=0;
						end if;
					end loop;
				if check_=1 then
					return 1;
				else
					check_=1;
				end if;
			end loop;
		END LOOP;
		return 0;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.delete_space_and_tab(
	text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
    text_line text;
 m   varchar;
 res text array;
 ret_txt text;
BEGIN
	if $1='' then
	return $1;
	end if;
	raise notice '%',$1;
   text_line=replace(replace(replace(replace($1,
        ' ', ' '),
        E'
', ' '),
        E'	', ' '),
        E'
', ' ');
   if LENGTH(text_line) > 0 then
	   FOREACH m IN ARRAY string_to_array(text_line,' ')
	   loop
		 if trim(m)!='' then
		  res=array_append(res,m);
		 end if;
		 end loop;
	else
	res=text_line;
	end if;
ret_txt:=array_to_string(res,' ');
return ret_txt;
END;
$BODY$;





	CREATE OR REPLACE FUNCTION public.generate_combinations(
	input_array text[])
    RETURNS text[]
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    result text[] := '{}';
    temp text[];
    i integer;
    j integer;
BEGIN
    -- Перебираем все элементы входного массива
    FOR i IN 1..array_length(input_array, 1) LOOP
        -- Разбиваем текущий элемент на подэлементы
        temp := string_to_array(input_array[i], '|');
        -- Если результирующий массив пустой, добавляем первый подэлемент
        IF result = '{}' THEN
            result := temp;
        ELSE
            -- Создаем временный массив для хранения новых комбинаций
            -- на основе текущего результата и текущего подэлемента
            temp := array(
                SELECT array_agg(elem1 || '|' || elem2)
                FROM (select unnest(result) AS elem1 limit 100000) AS elem1, (select unnest(temp) AS elem2 limit 100000) AS elem2
            );
            result := temp;
        END IF;
    END LOOP;

    RETURN result;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.remove_elements_from_array(
	arr1 text[],
	arr2 text[])
    RETURNS text[]
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE    result text[] := arr1;
BEGIN
    if arr1 is not null and arr2 is not null then
		FOR i IN 1..array_length(arr2, 1) LOOP        
		result := array_remove(result, arr2[i]);
		END LOOP;    
		RETURN result;
	else 
	return(arr1);
	end if;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.compare_arrays2(
	array1 text[],
	array2 text[])
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    comparison_elements text[];
BEGIN
	if array2 is null
	then
	RETURN 1;
	end if;
		
    -- Находим "элементы для сравнения", которые есть в массиве 1
    SELECT array_agg(element) INTO comparison_elements
    FROM unnest(array1) as element
    WHERE element = ANY (array2);
	
-- 	raise notice '%',comparison_elements;
-- 	raise notice '%',array_length(comparison_elements,1);
    
	if array_length(comparison_elements,1)>=1 then
        RETURN 1;
    ELSE
        RETURN 0;
    END IF;
END;
$BODY$;
"""
sql_indexes = """
create index elements_v  on elements (version);
create index labels_v  on labels (version);
create index linkbaserefs_v  on linkbaserefs (version);
create index locators_v  on locators (version);
create index rolerefs_v  on rolerefs (version);
create index roletypes_v  on roletypes (version);
create index rulenodes_v  on rulenodes (version);
create index rulenodes_c_v  on rulenodes_c (version);
create index rulenodes_e_v  on rulenodes_e (version);
create index rulenodes_p_v  on rulenodes_p (version);
create index tables_v  on tables (version);
create index tableschemas_v  on tableschemas (version);
create index va_assertions_v  on va_assertions (version);
create index va_concepts_v  on va_concepts (version);
create index va_edimensions_v  on va_edimensions (version);
create index va_edmembers_v  on va_edmembers (version);
create index va_factvars_v  on va_factvars (version);
create index va_tdimensions_v  on va_tdimensions (version);
create index va_generals_v  on va_generals (version);
create index tableparts_v  on tableparts (version);
create index va_aspectcovers_v  on va_aspectcovers (version);
create index va_assertionsets_v  on va_assertionsets (version);
create index aspectnodes_v  on aspectnodes (version);
create index entrypoints_v on entrypoints (version);
create index rend_edimensions_v  on rend_edimensions (version);
create index rend_edmembers_v  on rend_edmembers (version);
create index rulesets_v  on rulesets (version);
create index catalog_v  on catalog (version);
create index taxpackage_v  on taxpackage (version);

create index elements_entity  on elements (entity);
create index labels_entity  on labels (entity);
create index linkbaserefs_entity  on linkbaserefs (entity);
create index locators_entity  on locators (entity);
create index rolerefs_entity  on rolerefs (entity);
create index roletypes_entity  on roletypes (entity);
create index rulenodes_entity  on rulenodes (entity);
create index rulenodes_c_entity  on rulenodes_c (entity);
create index rulenodes_e_entity  on rulenodes_e (entity);
create index rulenodes_p_entity  on rulenodes_p (entity);
create index tables_entity  on tables (entity);
create index tableschemas_entity  on tableschemas (entity);
create index va_assertions_entity  on va_assertions (entity);
create index va_concepts_entity  on va_concepts (entity);
create index va_edimensions_entity  on va_edimensions (entity);
create index va_edmembers_entity  on va_edmembers (entity);
create index va_factvars_entity  on va_factvars (entity);
create index va_tdimensions_entity  on va_tdimensions (entity);
create index va_generals_entity  on va_generals (entity);
create index tableparts_entity  on tableparts (entity);
create index va_aspectcovers_entity  on va_aspectcovers (entity);
create index va_assertionsets_entity  on va_assertionsets (entity);
create index aspectnodes_entity  on aspectnodes (entity);
create index rend_edimensions_entity  on rend_edimensions (entity);
create index rend_edmembers_entity  on rend_edmembers (entity);
create index rulesets_entity  on rulesets (entity);

create index labels_parentrole  on labels (parentrole);
create index locators_parentrole  on locators (parentrole);
create index rulenodes_parentrole  on rulenodes (parentrole);
create index rulenodes_c_parentrole  on rulenodes_c (parentrole);
create index rulenodes_e_parentrole  on rulenodes_e (parentrole);
create index rulenodes_p_parentrole  on rulenodes_p (parentrole);
create index tableschemas_parentrole  on tableschemas (parentrole);
create index va_assertions_parentrole  on va_assertions (parentrole);
create index va_concepts_parentrole  on va_concepts (parentrole);
create index va_edimensions_parentrole  on va_edimensions (parentrole);
create index va_edmembers_parentrole  on va_edmembers (parentrole);
create index va_factvars_parentrole  on va_factvars (parentrole);
create index va_tdimensions_parentrole  on va_tdimensions (parentrole);
create index va_generals_parentrole  on va_generals (parentrole);
create index va_aspectcovers_parentrole  on va_aspectcovers (parentrole);
create index va_assertionsets_parentrole  on va_assertionsets (parentrole);
create index aspectnodes_parentrole  on aspectnodes (parentrole);
create index rend_edimensions_parentrole  on rend_edimensions (parentrole);
create index rend_edmembers_parentrole  on rend_edmembers (parentrole);
create index rulesets_parentrole  on rulesets (parentrole);

create index elements_rinok  on elements (rinok);
create index labels_rinok  on labels (rinok);
create index linkbaserefs_rinok  on linkbaserefs (rinok);
create index locators_rinok  on locators (rinok);
create index rolerefs_rinok  on rolerefs (rinok);
create index roletypes_rinok  on roletypes (rinok);
create index rulenodes_rinok  on rulenodes (rinok);
create index rulenodes_c_rinok  on rulenodes_c (rinok);
create index rulenodes_e_rinok  on rulenodes_e (rinok);
create index rulenodes_p_rinok  on rulenodes_p (rinok);
create index tables_rinok  on tables (rinok);
create index tableschemas_rinok  on tableschemas (rinok);
create index va_assertions_rinok  on va_assertions (rinok);
create index va_concepts_rinok  on va_concepts (rinok);
create index va_edimensions_rinok  on va_edimensions (rinok);
create index va_edmembers_rinok  on va_edmembers (rinok);
create index va_factvars_rinok  on va_factvars (rinok);
create index va_tdimensions_rinok  on va_tdimensions (rinok);
create index va_generals_rinok  on va_generals (rinok);
create index tableparts_rinok  on tableparts (rinok);
create index va_aspectcovers_rinok  on va_aspectcovers (rinok);
create index va_assertionsets_rinok  on va_assertionsets (rinok);
create index aspectnodes_rinok  on aspectnodes (rinok);
create index rend_edimensions_rinok  on rend_edimensions (rinok);
create index rend_edmembers_rinok  on rend_edmembers (rinok);
create index rulesets_rinok  on rulesets (rinok);

create index locators_href_id on locators (href_id);
create index locators_label on locators (label);
create index arcs_arcto on arcs (arcto);
create index arcs_arcfrom on arcs (arcfrom);

create index messages_v on messages (version);
create index messages_entity on messages (entity);
create index messages_parentrole on messages (parentrole);
create index messages_rinok on messages (rinok);
"""
sql_indexes_new = """
create index arcs_v  on arcs (to_tsvector('english'::regconfig,version));
create index elements_v  on elements (to_tsvector('english'::regconfig,version));
create index labels_v  on labels (to_tsvector('english'::regconfig,version));
create index linkbaserefs_v  on linkbaserefs (to_tsvector('english'::regconfig,version));
create index locators_v  on locators (to_tsvector('english'::regconfig,version));
create index rolerefs_v  on rolerefs (to_tsvector('english'::regconfig,version));
create index roletypes_v  on roletypes (to_tsvector('english'::regconfig,version));
create index rulenodes_v  on rulenodes (to_tsvector('english'::regconfig,version));
create index rulenodes_c_v  on rulenodes_c (to_tsvector('english'::regconfig,version));
create index rulenodes_e_v  on rulenodes_e (to_tsvector('english'::regconfig,version));
create index rulenodes_p_v  on rulenodes_p (to_tsvector('english'::regconfig,version));
create index tables_v  on tables (to_tsvector('english'::regconfig,version));
create index tableschemas_v  on tableschemas (to_tsvector('english'::regconfig,version));
create index va_assertions_v  on va_assertions (to_tsvector('english'::regconfig,version));
create index va_concepts_v  on va_concepts (to_tsvector('english'::regconfig,version));
create index va_edimensions_v  on va_edimensions (to_tsvector('english'::regconfig,version));
create index va_edmembers_v  on va_edmembers (to_tsvector('english'::regconfig,version));
create index va_factvars_v  on va_factvars (to_tsvector('english'::regconfig,version));
create index va_tdimensions_v  on va_tdimensions (to_tsvector('english'::regconfig,version));
create index va_generals_v  on va_generals (to_tsvector('english'::regconfig,version));
create index tableparts_v  on tableparts (to_tsvector('english'::regconfig,version));
create index va_aspectcovers_v  on va_aspectcovers (to_tsvector('english'::regconfig,version));
create index va_assertionsets_v  on va_assertionsets (to_tsvector('english'::regconfig,version));
create index aspectnodes_v  on aspectnodes (to_tsvector('english'::regconfig,version));
create index entrypoints_v on entrypoints (to_tsvector('english'::regconfig,version));
create index rend_edimensions_v  on rend_edimensions (to_tsvector('english'::regconfig,version));
create index rend_edmembers_v  on rend_edmembers (to_tsvector('english'::regconfig,version));
create index rulesets_v  on rulesets (to_tsvector('english'::regconfig,version));
create index catalog_v  on catalog (to_tsvector('english'::regconfig,version));
create index taxpackage_v  on taxpackage (to_tsvector('english'::regconfig,version));

create index arcs_entity  on arcs (to_tsvector('english'::regconfig,entity));
create index elements_entity  on elements (to_tsvector('english'::regconfig,entity));
create index labels_entity  on labels (to_tsvector('english'::regconfig,entity));
create index linkbaserefs_entity  on linkbaserefs (to_tsvector('english'::regconfig,entity));
create index locators_entity  on locators (to_tsvector('english'::regconfig,entity));
create index rolerefs_entity  on rolerefs (to_tsvector('english'::regconfig,entity));
create index roletypes_entity  on roletypes (to_tsvector('english'::regconfig,entity));
create index rulenodes_entity  on rulenodes (to_tsvector('english'::regconfig,entity));
create index rulenodes_c_entity  on rulenodes_c (to_tsvector('english'::regconfig,entity));
create index rulenodes_e_entity  on rulenodes_e (to_tsvector('english'::regconfig,entity));
create index rulenodes_p_entity  on rulenodes_p (to_tsvector('english'::regconfig,entity));
create index tables_entity  on tables (to_tsvector('english'::regconfig,entity));
create index tableschemas_entity  on tableschemas (to_tsvector('english'::regconfig,entity));
create index va_assertions_entity  on va_assertions (to_tsvector('english'::regconfig,entity));
create index va_concepts_entity  on va_concepts (to_tsvector('english'::regconfig,entity));
create index va_edimensions_entity  on va_edimensions (to_tsvector('english'::regconfig,entity));
create index va_edmembers_entity  on va_edmembers (to_tsvector('english'::regconfig,entity));
create index va_factvars_entity  on va_factvars (to_tsvector('english'::regconfig,entity));
create index va_tdimensions_entity  on va_tdimensions (to_tsvector('english'::regconfig,entity));
create index va_generals_entity  on va_generals (to_tsvector('english'::regconfig,entity));
create index tableparts_entity  on tableparts (to_tsvector('english'::regconfig,entity));
create index va_aspectcovers_entity  on va_aspectcovers (to_tsvector('english'::regconfig,entity));
create index va_assertionsets_entity  on va_assertionsets (to_tsvector('english'::regconfig,entity));
create index aspectnodes_entity  on aspectnodes (to_tsvector('english'::regconfig,entity));
create index rend_edimensions_entity  on rend_edimensions (to_tsvector('english'::regconfig,entity));
create index rend_edmembers_entity  on rend_edmembers (to_tsvector('english'::regconfig,entity));
create index rulesets_entity  on rulesets (to_tsvector('english'::regconfig,entity));

create index arcs_parentrole  on arcs (to_tsvector('english'::regconfig,parentrole));
create index labels_parentrole  on labels (to_tsvector('english'::regconfig,parentrole));
create index locators_parentrole  on locators (to_tsvector('english'::regconfig,parentrole));
create index rulenodes_parentrole  on rulenodes (to_tsvector('english'::regconfig,parentrole));
create index rulenodes_c_parentrole  on rulenodes_c (to_tsvector('english'::regconfig,parentrole));
create index rulenodes_e_parentrole  on rulenodes_e (to_tsvector('english'::regconfig,parentrole));
create index rulenodes_p_parentrole  on rulenodes_p (to_tsvector('english'::regconfig,parentrole));
create index tableschemas_parentrole  on tableschemas (to_tsvector('english'::regconfig,parentrole));
create index va_assertions_parentrole  on va_assertions (to_tsvector('english'::regconfig,parentrole));
create index va_concepts_parentrole  on va_concepts (to_tsvector('english'::regconfig,parentrole));
create index va_edimensions_parentrole  on va_edimensions (to_tsvector('english'::regconfig,parentrole));
create index va_edmembers_parentrole  on va_edmembers (to_tsvector('english'::regconfig,parentrole));
create index va_factvars_parentrole  on va_factvars (to_tsvector('english'::regconfig,parentrole));
create index va_tdimensions_parentrole  on va_tdimensions (to_tsvector('english'::regconfig,parentrole));
create index va_generals_parentrole  on va_generals (to_tsvector('english'::regconfig,parentrole));
create index va_aspectcovers_parentrole  on va_aspectcovers (to_tsvector('english'::regconfig,parentrole));
create index va_assertionsets_parentrole  on va_assertionsets (to_tsvector('english'::regconfig,parentrole));
create index aspectnodes_parentrole  on aspectnodes (to_tsvector('english'::regconfig,parentrole));
create index rend_edimensions_parentrole  on rend_edimensions (to_tsvector('english'::regconfig,parentrole));
create index rend_edmembers_parentrole  on rend_edmembers (to_tsvector('english'::regconfig,parentrole));
create index rulesets_parentrole  on rulesets (to_tsvector('english'::regconfig,parentrole));

create index arcs_rinok  on arcs (to_tsvector('english'::regconfig,rinok));
create index elements_rinok  on elements (to_tsvector('english'::regconfig,rinok));
create index labels_rinok  on labels (to_tsvector('english'::regconfig,rinok));
create index linkbaserefs_rinok  on linkbaserefs (to_tsvector('english'::regconfig,rinok));
create index locators_rinok  on locators (to_tsvector('english'::regconfig,rinok));
create index rolerefs_rinok  on rolerefs (to_tsvector('english'::regconfig,rinok));
create index roletypes_rinok  on roletypes (to_tsvector('english'::regconfig,rinok));
create index rulenodes_rinok  on rulenodes (to_tsvector('english'::regconfig,rinok));
create index rulenodes_c_rinok  on rulenodes_c (to_tsvector('english'::regconfig,rinok));
create index rulenodes_e_rinok  on rulenodes_e (to_tsvector('english'::regconfig,rinok));
create index rulenodes_p_rinok  on rulenodes_p (to_tsvector('english'::regconfig,rinok));
create index tables_rinok  on tables (to_tsvector('english'::regconfig,rinok));
create index tableschemas_rinok  on tableschemas (to_tsvector('english'::regconfig,rinok));
create index va_assertions_rinok  on va_assertions (to_tsvector('english'::regconfig,rinok));
create index va_concepts_rinok  on va_concepts (to_tsvector('english'::regconfig,rinok));
create index va_edimensions_rinok  on va_edimensions (to_tsvector('english'::regconfig,rinok));
create index va_edmembers_rinok  on va_edmembers (to_tsvector('english'::regconfig,rinok));
create index va_factvars_rinok  on va_factvars (to_tsvector('english'::regconfig,rinok));
create index va_tdimensions_rinok  on va_tdimensions (to_tsvector('english'::regconfig,rinok));
create index va_generals_rinok  on va_generals (to_tsvector('english'::regconfig,rinok));
create index tableparts_rinok  on tableparts (to_tsvector('english'::regconfig,rinok));
create index va_aspectcovers_rinok  on va_aspectcovers (to_tsvector('english'::regconfig,rinok));
create index va_assertionsets_rinok  on va_assertionsets (to_tsvector('english'::regconfig,rinok));
create index aspectnodes_rinok  on aspectnodes (to_tsvector('english'::regconfig,rinok));
create index rend_edimensions_rinok  on rend_edimensions (to_tsvector('english'::regconfig,rinok));
create index rend_edmembers_rinok  on rend_edmembers (to_tsvector('english'::regconfig,rinok));
create index rulesets_rinok  on rulesets (to_tsvector('english'::regconfig,rinok));

create index locators_href_id on locators (to_tsvector('english'::regconfig,href_id));
create index locators_label on locators (to_tsvector('english'::regconfig,label));
create index arcs_arcto on arcs (to_tsvector('english'::regconfig,arcto));
create index arcs_arcfrom on arcs (to_tsvector('english'::regconfig,arcfrom));

create index messages_v on messages (to_tsvector('english'::regconfig,version));
create index messages_entity on messages (to_tsvector('english'::regconfig,entity));
create index messages_parentrole on messages (to_tsvector('english'::regconfig,parentrole));
create index messages_rinok on messages (to_tsvector('english'::regconfig,rinok));
"""
sql_create_elements_labels = """
create table elements_labels as 
--insert into elements_labels
select e.version,e.rinok,la.entity label_entity,la.label label_id,e.entity,e.name,e.id,e.qname,e.type,e.substitutiongroup,la.lang,la.label,la.role,e.abstract,la.text,full_path
from 
elements e 
left join locators le ON le.href_id = e.id AND le.rinok = e.rinok AND le.locfrom = 'label' and le.version=e.version
left join arcs ae ON ae.rinok = le.rinok AND ae.entity = le.entity AND ae.arcfrom = le.label 
AND ae.arctype = 'label' and ae.version=le.version
left join labels la ON la.rinok = ae.rinok AND la.entity = ae.entity AND la.label = ae.arcto and la.version=ae.version
"""
sql_create_preferred_labels = """
create table preferred_labels as
select a.version,a.rinok,a.entity,a.parentrole,l.href_id id,el.qname,el.role,el.abstract,el.type,el.substitutiongroup,text,full_path,label_id
from arcs a
join locators l on l.label=a.arcto and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole	
join elements_labels el on el.version=l.version and el.id=l.href_id
where arctype='presentation' and preferredlabel is not null
and el.role=a.preferredlabel;
"""
sql_create_dop_tables = """
CREATE TABLE IF NOT EXISTS public.roles_table_definition
(
    role_definition text COLLATE pg_catalog."default",
    role_table text COLLATE pg_catalog."default"
)
"""

