# AGENTS.md

## Writing standard: ASD-STE100 Simplified Technical English

Use Simplified Technical English (ASD-STE100) for all English prose that you write. This includes chat replies, commit messages, PR descriptions, code comments, docstrings, README files, documentation, error strings, and log messages. Write in the active voice and use the imperative for instructions. Write one instruction in one sentence. Keep a procedural sentence to 20 words and a descriptive sentence to 25 words. Use only the simple present, past, and future tenses. Use one word for one meaning, prefer the short common word, and do not use idioms, humor, or jargon. Write "must" and "do not" for a requirement. Write an abbreviation in full at its first use.

Do not apply this standard to code, identifiers, API signatures, quoted output from other programs, product names, or text that a person wrote. Do not rename an existing symbol only to obey this standard.

## Commit messages and PR descriptions

Keep a commit to one change, and make the commit only when it is complete and correct. Write the subject line in the imperative mood, start it with a capital letter, and keep it to 50 characters or fewer. Do not end the subject line with a period. Write a body when the reason is not clear from the subject. Wrap the body at 72 characters. The body says why the change is necessary and what it affects. Do not write design history or alternatives in the body. Write every commit message in Simplified Technical English, per the standard above.

Keep a PR to one change, and open it only when the code builds and the tests pass. Write the title like a commit subject line. Start the description with one sentence that says why the change is necessary. After that sentence, list what the change affects. Link the issue that the PR closes. Do not write design history, alternatives, or a restatement of the diff in the description. Write every PR title and description in Simplified Technical English, per the standard above.

## Docstring length

Keep every docstring and comment to a maximum of two lines. Write what the reader cannot get from the name and the signature: the constraint, the failure mode, or the reason. Delete a docstring that only repeats the name. Do not write design history, alternatives, or future plans in a docstring.
