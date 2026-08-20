# AGENTS.md

## Writing standard: ASD-STE100 Simplified Technical English

Use Simplified Technical English (ASD-STE100) for all English prose that you write. This includes chat replies, commit messages, PR descriptions, code comments, docstrings, README files, documentation, error strings, and log messages. Write in the active voice and use the imperative for instructions. Write one instruction in one sentence. Keep a procedural sentence to 20 words and a descriptive sentence to 25 words. Use only the simple present, past, and future tenses. Use one word for one meaning, prefer the short common word, and do not use idioms, humor, or jargon. Write "must" and "do not" for a requirement. Write an abbreviation in full at its first use.

Do not apply this standard to code, identifiers, API signatures, quoted output from other programs, product names, or text that a person wrote. Do not rename an existing symbol only to obey this standard.

## Docstring length

Keep every docstring and comment to a maximum of two lines. Write what the reader cannot get from the name and the signature: the constraint, the failure mode, or the reason. Delete a docstring that only repeats the name. Do not write design history, alternatives, or future plans in a docstring.
