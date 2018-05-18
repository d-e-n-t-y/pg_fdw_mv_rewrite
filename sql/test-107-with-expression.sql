explain (VERBOSE, COSTS OFF) select key, case when count(value) < 10 then 'several'
        when count(value) < 100 then 'tens'
        when count(value) < 1000 then 'hundreds'
        when count(value) < 10000 then 'thousands'
        when count(value) < 100000 then 'tens of thousands'
        when count(value) < 1000000 then 'hundreds of thousands'
        when count(value) < 10000000 then 'millions'
        else 'many'
    end as quantity from test group by key;

select key, case when count(value) < 10 then 'several'
        when count(value) < 100 then 'tens'
        when count(value) < 1000 then 'hundreds'
        when count(value) < 10000 then 'thousands'
        when count(value) < 100000 then 'tens of thousands'
        when count(value) < 1000000 then 'hundreds of thousands'
        when count(value) < 10000000 then 'millions'
        else 'many'
    end as quantity from test group by key;
