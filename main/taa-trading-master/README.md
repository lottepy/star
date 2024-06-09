# TAA-trading

live trading of TAA strategies

## Event Driven:
IB:
` python -m algorithm.t0example -cp utils/configs/config_ib.ini --iuid GB_40_EURUSD`

寬睿:
`python -m algorithm.t0example -cp utils/configs/config.ini --iuid CN_10_000001`

# Update submodule:
- First sync with remote head: `git pull`

- Update upto current head tracked commit:
  - `git submodule update`

- Update to the latest commit:
  - `git submodule update --remote`.
  - then commit submodule:
    - `git add lib/commonalgo`
    - `git commit -m "update commonalgo"`
    - `git push`
