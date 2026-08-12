from semantic_search.segment_chunking import chunk_transcript_segments


def _segment(start, end, word_count):
    return {"start": start, "end": end, "text": " ".join(["word"] * word_count)}


def test_aggregates_until_target_word_count():
    segments = [
        _segment(0, 10, 100),
        _segment(10, 20, 100),
        _segment(20, 30, 100),
        _segment(30, 40, 100),
    ]
    chunks = chunk_transcript_segments(segments, target_words=250)

    # word_count crosses 250 partway through the 3rd segment (100+100+100=300),
    # so the first chunk absorbs segments 0-2 and flushes there.
    assert len(chunks) == 2
    assert chunks[0].start == 0
    assert chunks[0].end == 30
    assert chunks[1].start == 30
    assert chunks[1].end == 40


def test_empty_segments_produce_no_chunks():
    assert chunk_transcript_segments([]) == []


def test_skips_empty_text_segments():
    segments = [_segment(0, 10, 5), {"start": 10, "end": 12, "text": ""}, _segment(12, 20, 5)]
    chunks = chunk_transcript_segments(segments, target_words=1000)
    assert len(chunks) == 1
    assert chunks[0].text.count("word") == 10


def test_chunk_index_is_sequential():
    segments = [_segment(i * 10, i * 10 + 10, 300) for i in range(3)]
    chunks = chunk_transcript_segments(segments, target_words=250)
    assert [c.chunk_index for c in chunks] == list(range(len(chunks)))
