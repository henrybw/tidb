# On Code Review

## Background

These are some notes on how I (@henrybw) approach code reviews. I've documented the general process that I follow when I perform code reviews, along with the principles and philosophy behind my approach, based on what I've found works particularly well for fostering productive code reviews.

I generally strive to be thorough and comprehensive when I perform code reviews.

(While I've tried to capture and document the most important points I could think of, this is probably not an exhaustive or complete list.)

## Process

First, read the description (problem summary, what changed and how does it work) to understand the goal of the PR.

Then, review the changes in multiple passes:

1. Skim the changes to get a high-level understanding of what is going on. Does it seem to follow from the intent conveyed by the PR description?
  * Note any areas being changed where the code is unfamiliar. For each of these, go read the surrounding code to build an understanding of the context for the change, or ask the author clarifying questions.
  * Does the overall approach seem reasonable? Do the changes roughly make sense? If not, this is an issue that needs to be raised.
2. Close reading of the code. For each change:
  * If existing code is being modified or removed, ensure you understand what the existing code did, and why it is being removed. If that is not clear, raise an issue.
  * For new code, closely read each line, as if you had to modify or refactor it yourself. Can you follow what the code is doing? (If not, this is an issue that needs to be addressed.) Does the code make sense? Does it follow from what the PR described?
3. Critical thinking and suggestions. Are there specific, tangible ways to improve the code being changed?
  * Necessity: Does the code directly address the problem being solved, or are there unnecessary complications that can be simplified? Does the code duplicate or reimplement any logic or services provided by other functions/modules/components? (If so, the code is unnecessarily redundant, and should leverage or improve the functionality that already exists.)
  * Sufficiency: Does the code fully address what the PR says it does? Are there cases or usages that were missed or not considered?
  * Robustness: Try to imagine ways the new code could break or fail (including performance and scalability bottlenecks). Are failure modes reasonably addressed?
  * Usability: Try to imagine how the code will be used, either by end-users or other parts of the codebase. Would things make sense to someone who is not familiar with the code and its specific implementation details?
  * Style: Are there simpler, more direct, or even more elegant ways to implement parts of the PR? (Usually these should just be suggestions rather than blocking issues, unless the implementation is especially confusing, unnecessarily complicated, or difficult to understand.)

## Principles

* **The reviewer must read and understand every change the author is making.**
  * **If this is not feasible, then it means the PR is too big.** The author should break up the PR into smaller PRs that are feasible for reviewers to understand in detail.
    * A good rule of thumb is to scope each PR as one minimal, atomic change. Large features can then be implemented as a "stack" of PRs that build on top of each other.
    * Another good rule of thumb is to separate cleanup and refactoring changes into a separate PR. This way, they don't add clutter/noise to PRs that are making semantic changes.
  * Some types of changes, especially from cleanups or refactors, are more mechanical (for example, renaming every usage of a field). Every instance should still be reviewed, but they don't need the same scrutiny as more semantic code changes.
  * Comments must also be reviewed for correctness and worth/necessity ("if this comment didn't exist, would it make the code more difficult to understand?").
    * Also consider the likelihood of a comment going stale, especially if it refers to something else that must be kept in sync. A stale and misleading comment can be more harmful than useful to future readers of the code.
* **Both the author and reviewer should articulate *clear and justifiable* reasons for their decisions and feedback.**
  * **The reviewer should review the code based on its own merits, not based on how the reviewer would have personally written the code.**
  * Generally, the reviewer should take the perspective of someone who has to read and understand the code in the future, not the perspective of someone writing code now.
  * The reviewer can still offer suggestions based on how they would have written the code, if they think it is a substantial improvement, and they can articulate clear reasons for why it is better. But the reviewer should not block a PR just because code is not written in the same way the reviewer would have written it.
  * Evidence and technical reasoning should override preference and personal opinion.
* **The author must at least respond to all feedback raised by the reviewer, and must address all blocking issues.**
  * Suggestions from the reviewer should generally be incoporated, unless the author (or other reviewers) can clearly articulate a good reason for not incorporating the suggestion (and the reviewer agrees).
    * As an author, I generally prefer to lean towards doing what the reviewer asks, even if it's not what I would have done personally.
  * The author and reviewer should generally try to resolve disagreements between themselves without external intervention. In extreme cases, if the author and reviewer cannot resolve a dispute, then they should escalate the discussion to a tech lead, or the broader team.
    * Hopefully an escalation like this rarely has to happen. (I have never seen it happen in my recent experience: usually the reviewer and author have been able to resolve disputes on their own.)
* **The reviewer should never approve a PR if there are outstanding blocking issues.**
  * Separate review feedback into three tiers: blocking issues, suggestions, and nit picks. These decrease in importance.
    * **All blocking issues must be addressed or have a resolution/compromise worked out with the reviewer.**
  * Suggestions and nit picks can be non-blocking, less important forms of feedback. It's up to the author to incorporate these forms of feedback, and it should not matter to the reviewer if they are addressed or not.
    * A suggestion might be, for example, a different name for a variable or field that the reviewer thinks is clearer or more idiomatic. These can be blocking (for example, if the name is confusing or doesn't make sense), or non-blocking (if the existing name is fine, but could be improved). The reviewer should make their expectations clear.
    * A nit pick might be, for example, fixing a typo or awkwardly-phrased comment, removing extraneous parentheses around a Boolean condition, or simplifying nested if conditionals with a Boolean && operator.
      * That said, the reviewer should be careful to not use their personal preferences as a reason to excessively nit pick.
  * If a review is time-sensitive, the author should make this expectation clear, and the reviewer should take this into account ("is this really important enough to block my approval of the PR?").
    * **However, the author must never rush or pressure a reviewer to approve a PR due to time constraints, and the reviewer should never give into such demands.**
* **Both the author and reviewer must always be courteous, polite, and respectful to each other.**
  * The author and reviewer should have the same goal: to collaboratively produce the best, highest quality change they can.
  * The reviewer should be encouraged to raise issues, even if they indicate serious flaws that would require substantial rewrites from the author.
  * **The reviewer should take care to ensure their feedback is about the *code*, not the *author*. Feedback should *never* be personal.**
    * A good rule of thumb is use "we" rather than "you": for example, "We shouldn't be calling this repeatedly" is better than "You shouldn't be calling this repeatedly". This both avoids the risk of interpreting feedback as personal attacks, frames code review as a collaborative effort between the author and the reviewer, and emphasizes the shared ownership of the codebase.
* **Both the author and reviewer should respect each other's time.**
  * The reviewer should not have to repeatedly push the author to address similar issues that they have previously pointed out. If the reviewer has pointed out an issue in one part of the PR, the author should then proactively look for and address similar issues in the rest of the PR before re-requesting a review.
  * When raising an issue, the reviewer should be conscientious of how much work they are asking the author to do, and whether it would remain in the scope of the PR. For example, if there are other, existing problems in the area of the codebase that the author is modifying, the reviewer shouldn't make the author fix those tangentially-related problems.
    * As a compromise, the reviewer could ask the author to create a follow-up PR to address issues that are not in scope of the PR being reviewed.

## External Resources

* [The Gentle Art of Patch Review: The Three-Phase Contribution Review][1]
  * Interestingly, I had not read this until recently, long after I had been reviewing in "multiple passes". We seem to have arrived at similar conclusions independently.
* [How to do a code review — Google’s Code Review Guidelines][2]
* [LLVM Code-Review Policy and Practices][3]
  * [LLVM: Review expectations (for first-time contributors)][4]

[1]: https://sage.thesharps.us/2014/09/01/the-gentle-art-of-patch-review/
[2]: https://google.github.io/eng-practices/review/reviewer/
[3]: https://llvm.org/docs/CodeReview.html
[4]: https://llvm.org/docs/MyFirstTypoFix.html#review-expectations
