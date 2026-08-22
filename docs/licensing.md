# Licensing decision record

## Status

No license has been selected and the repository intentionally has no `LICENSE`
file. Publicly visible source is not automatically open source; without an
explicit license, copyright law generally reserves copying, modification, and
redistribution rights.

Only the repository owner can make the licensing decision. Contributors and
automation must not add a license based on an assumption.

## Owner decision required

Before a distributable release, the owner should decide and record:

- whether the project should be open source, source-available, proprietary, or
  remain undistributed;
- whether they have the right to license every source file and contribution;
- desired permissions for commercial use, modification, redistribution, patent
  rights, and private use;
- desired conditions such as attribution, notice preservation, disclosure of
  source for derivatives, or network-use provisions;
- warranty/liability posture and organizational policy;
- compatibility with all direct and transitive dependency licenses;
- whether documentation, test fixtures, model prompts, and configuration use the
  same license;
- whether any data, Reddit content, provider output, personal document, logo, or
  third-party asset must be excluded because a software license cannot grant
  rights to it.

The owner should obtain legal advice where commercial distribution, copyleft,
patents, employer ownership, or third-party data rights are material.

## Release action

After approval:

1. add the exact approved license text as `LICENSE`;
2. add the copyright holder and year only as approved;
3. update the README and package metadata license fields/classifiers;
4. generate and review a dependency-license inventory;
5. add required notices or source-offer material;
6. document that the software license does not license collected Reddit content,
   credentials, model output, or external datasets;
7. record the decision in the changelog and release review.

Until these steps are complete, do not describe the project as MIT, Apache,
GPL, open source, or freely reusable.
