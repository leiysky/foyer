# Vendored source

`foyer-extent` and `foyer-fixed-lsm` were imported into the leiysky Foyer fork on 2026-07-19 from
the standalone Extent prototype developed alongside ScopeDB. This in-tree copy is now the source
of truth for Foyer integration; changes should be made and tested here rather than synchronized by
copying files from the prototype workspace.

Both packages retain the original ScopeDB proprietary license in their local `LICENSE` files and
are excluded from Foyer's Apache-2.0 header rule. They are workspace packages but are not published
to crates.io.
