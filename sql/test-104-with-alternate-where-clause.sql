explain (VERBOSE, COSTS OFF) select key, COUNT (value) from test_lb where hidden = 'hidden3' group by key;

-- FIXME: although the balooning of WHERE cluases seems harmless, we should find out why it is happening.
select key, COUNT (value) from test_lb where hidden = 'hidden3' group by key;
