from datagouvfr_data_pipelines.utils.utils import get_unique_list


def test_get_unique_list():
    unique_list = get_unique_list(
        ["hello", "world", "this", "is", "Mars", "!"],
        ["hello", "Mars", "this", "is", "Earth", "!"],
    )
    expected_result = ["hello", "Mars", "is", "this", "Earth", "world", "!"]

    assert len(unique_list) == len(expected_result)
    assert all(x in expected_result for x in unique_list)
