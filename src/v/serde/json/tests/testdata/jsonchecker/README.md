Test suite from http://json.org/JSON_checker/.

If the JSON_checker is working correctly, it must accept all of the pass*.json files and reject all of the fail*.json files.

Changes:
- fail18.json is removed because it expects depth limits which we don't implement.
