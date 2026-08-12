from semantic_search.hybrid import reciprocal_rank_fusion


def test_item_in_both_lists_outranks_single_list_item():
    scores = reciprocal_rank_fusion([[1, 2, 3], [2, 4, 5]])
    # episode 2 appears in both lists (rank 1 and rank 0) — should score highest.
    assert max(scores, key=scores.get) == 2


def test_top_of_one_list_beats_bottom_of_the_other_when_absent_elsewhere():
    scores = reciprocal_rank_fusion([[1, 2, 3], []])
    assert scores[1] > scores[2] > scores[3]


def test_empty_lists_produce_empty_scores():
    assert reciprocal_rank_fusion([[], []]) == {}


def test_k_parameter_shrinks_rank_position_influence():
    small_k = reciprocal_rank_fusion([[1, 2]], k=1)
    large_k = reciprocal_rank_fusion([[1, 2]], k=1000)
    # with a small k, rank 0 vs rank 1 differs a lot; with a huge k, they're nearly equal.
    small_gap = small_k[1] - small_k[2]
    large_gap = large_k[1] - large_k[2]
    assert small_gap > large_gap


def test_score_matches_formula():
    scores = reciprocal_rank_fusion([[7]], k=60)
    assert scores[7] == 1 / 60
