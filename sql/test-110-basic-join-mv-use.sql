explain (VERBOSE, COSTS OFF)
select n.name, n.class, v.additional, count(value) as count
FROM test_name_lb n, test_value_lb v
WHERE n.id1 = v.id1 and n.id2 = v.id2
GROUP BY n.name, n.class, v.additional;

-- FIXME: the above doesn't select the MV, most likely because the MV doesn't list the WHERE clause params in its tList, hence it is not possible for the vars to match up (when run with "server loopback options (add log_match_progress 'true')", it notes: "check_from_join_clauses_for_matview: expression not found in grouped rel: {OPEXPR :opno 1752 :opfuncid 1718 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args ({VAR :varno 1 :varattno 1 :vartype 1700 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 1 :location 137} {VAR :varno 2 :varattno 1 :vartype 1700 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 2 :varoattno 1 :location 145}) :location 143}")'

--select n.name, n.class, v.additional, count(value) as count
--FROM test_name_lb n, test_value_lb v
--WHERE n.id1 = v.id1 and n.id2 = v.id2
--GROUP BY n.name, n.class, v.additional;
