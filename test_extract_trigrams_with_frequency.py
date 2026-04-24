import pytest

from classes.class_comparison import extract_trigrams_with_frequency


@pytest.mark.parametrize(
    "text, expected",
    [
        ("banana", {("ban", 1), ("ana", 2), ("nan", 1)}),
        ("ab", set()),
        ("", set()),
        ("aaaa", {("aaa", 2)}),
    ],
)
def test_extract_trigrams_with_frequency(text, expected):
    assert extract_trigrams_with_frequency(text) == expected


def test_extract_trigrams_with_frequency_requires_string():
    with pytest.raises(TypeError):
        extract_trigrams_with_frequency(None)
