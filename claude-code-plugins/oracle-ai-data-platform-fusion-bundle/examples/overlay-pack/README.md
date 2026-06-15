# Overlay Pack Example

This is a minimal additive overlay for the shipped `fusion-finance-starter`
content pack. It adds one new gold mart, `supplier_spend_by_currency`, without
modifying the starter pack.

Copy the overlay into a customer bundle and wire it with:

```bash
aidp-fusion-bundle use-pack examples/overlay-pack --profile finance-default
aidp-fusion-bundle content-pack validate examples/overlay-pack
aidp-fusion-bundle run --mode seed --datasets supplier_spend_by_currency --layers gold --dry-run
```

In a real customer project, keep overlays under that project's `overlays/`
directory, for example `overlays/supplier-currency-summary/`.

After the new mart is seeded, run `oac-dataset-advisor` again so it can
recommend the OAC dataset over the live gold table.
