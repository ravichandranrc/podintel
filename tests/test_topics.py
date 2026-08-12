from intelligence.topics import slugify


def test_slugify_lowercases_and_hyphenates():
    assert slugify("AI Agents") == "ai-agents"


def test_slugify_folds_whitespace_and_punctuation():
    assert slugify("  Retrieval-Augmented   Generation! ") == "retrieval-augmented-generation"


def test_slugify_is_idempotent_for_near_duplicates():
    # "AI Agents" and "ai   agents" should collide onto the same slug —
    # this is the "good enough" cross-episode consistency DESIGN.md §6 relies on.
    assert slugify("AI Agents") == slugify("ai   agents")


def test_slugify_empty_string():
    assert slugify("   ") == ""
