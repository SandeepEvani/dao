# DataObject Properties in Route Resolution — Design Analysis

> **Date:** 2026-02-28  
> **Status:** Experimental / RFC  
> **Scope:** `Router`, `DataAccessor`, `DataObject`, `@when` decorator

---

## Table of Contents

- [1. Background](#1-background)
- [2. Problem Statement](#2-problem-statement)
- [3. Current Routing Flow](#3-current-routing-flow)
- [4. Approaches](#4-approaches)
  - [4.1 Approach 1: Flat Merge Before Routing](#41-approach-1-flat-merge-before-routing)
  - [4.2 Approach 2: routing\_properties() Method on DataObject](#42-approach-2-routing_properties-method-on-dataobject)
  - [4.3 Approach 3: Inject an All-Round Context Variable](#43-approach-3-inject-an-all-round-context-variable)
  - [4.4 Approach 4: Post-Routing Default Enrichment](#44-approach-4-post-routing-default-enrichment)
  - [4.5 Approach 5: Per-Route Signature-Aware Property Injection](#45-approach-5-per-route-signature-aware-property-injection)
- [5. Comparative Scoring](#5-comparative-scoring)
- [6. Key Differences: Approach 4 vs Approach 5](#6-key-differences-approach-4-vs-approach-5)
- [7. Implementation Steps (Approach 5)](#7-implementation-steps-approach-5)
- [8. Further Considerations & Hybrids](#8-further-considerations--hybrids)

---

## 1. Background

The DAO library routes data access operations (`read`, `write`, etc.) to interface methods
using a `Router` that matches based on:

- **Structural compatibility** — argument count matches the method signature
  (`_check_structure` in `router.py`).
- **Type compatibility** — argument types match the method's type annotations
  (`_check_types`).
- **`@when` conditions** — conditional dispatch based on argument values
  (`_matches_conditions`).

Currently, route resolution only considers **arguments explicitly passed by the user** at
the call site (e.g., `dao.read(data_object=obj, path="s3://...")`). The `DataObject`'s own
properties (like `schema`, `suffix`, `columns`, etc.) are **not** visible to the router.

The question: **should DataObject properties participate in route resolution, and if so, how?**

---

## 2. Problem Statement

A `DataObject` carries rich metadata — schema, format, columns, keys, and arbitrary
`**properties` from catalogs or manual construction. Interface authors may want to
route differently based on what the data object *is* (e.g., its `suffix` or `schema`),
not just what the user *passes*.

However, injecting these properties into routing introduces risks:

- **Unpredictability** — users may not know what properties their data object carries.
- **Structural breakage** — inflated arg counts break exact-match checks in non-`**kwargs`
  methods.
- **Catalog instability** — `FileCatalog`-created objects have arbitrary JSON-driven
  properties.
- **Namespace collisions** — DataObject internals (`_name`, `_data_store`) or DataStore
  properties (`bucket`, `region`) leaking into the routing pool.

---

## 3. Current Routing Flow

```
User calls dao.read(data_object=obj, path="s3://...")
        │
        ▼
DataAccessor._()
        │
        ├── segregate_args()           → method_args, conf_args
        ├── _extract_data_store_info() → data_store, data_class
        ├── _initialize_interface_if_needed()
        │
        ▼
Router.choose_route(method_args, data_store.name, conf_args)
        │
        ├── filter_routes(data_store, len(args))
        │       └── route["length_non_var_args"] <= arg_length
        │
        ├── get_route(search_space, args, confs)
        │       ├── _is_compatible_route(route, args)
        │       │       ├── _check_structure(signature, effective_args)
        │       │       │       ├── len_all_args == len(args)  [non-varargs]
        │       │       │       └── non_var_args ⊆ args
        │       │       └── _check_types(signature, effective_args)
        │       │               └── typeguard check per param
        │       └── _matches_conditions(route, args)
        │               └── @when conditions ⊆ args
        │
        ▼
accessor builds call_args (strip routing hints) → route["method"](**call_args)
```

### Key Files

| File | Role |
|---|---|
| `src/dao/core/accessor.py` | Orchestrates the DAO call flow |
| `src/dao/core/router.py` | Route matching (structure, types, `@when`) |
| `src/dao/core/signature/` | Signature analysis and combinatorial expansion |
| `src/dao/data_object/data_object.py` | Base DataObject (carries properties) |
| `src/dao/data_object/table_object.py` | TableObject subclass (schema, columns, etc.) |
| `src/dao/decorators.py` | `@when`, `@register` decorators |
| `src/dao/catalog/file.py` | FileCatalog — creates DataObjects from JSON config |

---

## 4. Approaches

### 4.1 Approach 1: Flat Merge Before Routing

**What it does:** In `accessor.py` `_()`, flatten `vars(data_object)` into `method_args`
before passing to the router:

```python
data_object_vars = vars(data_object)
method_args = {**data_object_vars, **method_args}
```

Both `choose_route` and the final `route["method"](**call_args)` see the merged dict.

#### Pros

- **Simplest implementation** — two lines of code.
- Both routing (`@when`, structural matching, type matching) and the method body get
  the enriched context in one shot.

#### Cons

- **Breaks structural routing.** `_check_structure` does
  `signature.len_all_args != len(args)` for non-varargs methods. A `TableObject` injects
  `_name`, `_data_store`, `name`, `data_store`, `identifier`, `schema`, `columns`,
  `primary_keys` — 8+ extra keys. Any interface method without `**kwargs` will fail this
  check.
- **Private attrs (`_name`, `_data_store`) leak** into routing and the method call. The
  method would receive `_name=...` as a kwarg — likely causing `TypeError`.
- **DataStore properties bleed in.** `DataObject.__init__` copies non-underscore DataStore
  attrs onto itself. So `vars(data_object)` also contains things like `bucket`, `region`.
- **Unpredictable `@when` matching.** A catalog-created object gets its properties from JSON
  config — `key`, `description`, etc. These silently participate in `@when` evaluation and
  structural matching. The user calling `dao.read(data_object=obj)` has no idea
  `description` is influencing routing.
- **Method call breaks.** After routing, `call_args` contains all merged keys. Unless
  *every* interface method uses `**kwargs`, the call `route["method"](**call_args)` will
  raise `TypeError: unexpected keyword argument`.

#### Catalog Impact

Particularly dangerous. A JSON-configured object (`{"key": "...", "description": "..."}`)
via `FileCatalog` produces arbitrary properties. The set of routing-visible keys is entirely
config-driven and invisible to the user.

---

### 4.2 Approach 2: `routing_properties()` Method on DataObject

**What it does:** Add a `routing_properties()` method to `DataObject` that returns a curated
dict. Subclass authors override it to declare which properties participate in routing.

#### Pros

- Explicit and predictable for hand-crafted `DataObject` subclasses like `S3FileObject`,
  `TableObject`.
- Clean separation — the object author decides what's visible.

#### Cons

- **Doesn't scale to catalog patterns.** Generic `DataObject` created from JSON config
  (via `FileCatalog.get_data_object`) has no subclass to override. You'd need a way to
  classify which JSON keys are "routable" vs. not — pushing complexity into config schema
  (`{"key": "...", "routable": true}`), which is fragile and couples config to routing
  internals.
- Still has the structural matching and method-call problems from Approach 1 (just with
  fewer keys).
- Adds a contract that every subclass must maintain.

---

### 4.3 Approach 3: Inject an All-Round Context Variable

**What it does:** Instead of merging into `method_args`, create a separate context object
(e.g., `DAOContext`) that bundles `method_args` + `data_object_vars` + metadata, and pass
it to the interface method as a single parameter (or via `contextvars`).

#### Pros

- **Zero impact on routing.** `method_args` stays pristine — `choose_route` sees only
  what the user explicitly passed.
- **The interface method gets everything.** The method body can access
  `context.data_object.schema`, `context.args["path"]`, etc.
- **Catalog-safe.** Doesn't matter what properties the catalog injected.

#### Cons

- **Changes the interface method contract.** Interface methods would need to accept a
  context parameter or use `contextvars`. This is a breaking API change.
- If using `contextvars`/thread-local: adds implicit state, harder to test.
- If using explicit `context` param: every interface method signature changes, breaking
  all existing interfaces.
- **DataObject properties can't influence `@when` routing** — you lose the ability to
  route based on what the object *is*.

#### Variant — Hybrid

Pass context via `contextvars` (non-breaking); interface authors opt-in by importing the
context. Routing stays clean, access is available.

---

### 4.4 Approach 4: Post-Routing Default Enrichment

**What it does:** Let route selection happen with *only* user-provided args. After a route
is chosen, look at the selected method's signature for parameters that have defaults (or
are optional) and were *not* provided by the user. If a matching key exists in
`vars(data_object)`, fill it in before calling the method.

Concretely, in `accessor.py _()` after `route = self._get_routed_method(...)`: inspect
`route["signature"].parameters` for params not in `method_args`, and pull matching values
from the data object.

#### Pros

- **Routing is 100% predictable.** `choose_route` sees exactly what the user passed — no
  inflation, no phantom keys. `@when`, `_check_structure`, `_check_types` all behave as
  today.
- **Method gets enriched args seamlessly.** If `read_data(self, data_object, schema=None)`
  is the chosen route and the user didn't pass `schema`, but
  `data_object.schema = "public"` — the method receives `schema="public"` automatically.
- **Catalog-safe.** Only properties matching the *selected method's declared parameters*
  are injected.
- **Non-breaking.** Existing interfaces work unchanged.
- **Opt-in by design.** An interface method "opts in" to receiving data object properties
  by declaring a parameter with a default value whose name matches a DataObject property.

#### Cons

- **Implicit behavior — but bounded.** A user unfamiliar with the DataObject's properties
  might not realize `schema` came from the object rather than being the method's default.
- **Name coincidence risk.** If a method has an optional parameter named `columns` and the
  DataObject also has `columns`, enrichment happens silently.
- **Doesn't help with `@when` routing.** If you want DataObject properties to influence
  route *selection*, this approach explicitly cannot do that.
- **`**kwargs` policy ambiguity.** Should a `**kwargs` method receive all data object
  properties? Needs a clear decision.

---

### 4.5 Approach 5: Per-Route Signature-Aware Property Injection

**What it does:** Pass `method_args` and `properties` (from `vars(data_object)`) as
**separate dicts** to the router. During route matching, for each candidate route:

1. **Route's signature only has params from `method_args`** → use `method_args` alone.
   Properties are irrelevant.
2. **Route's signature has params whose keys also exist in data object properties** →
   merge those specific properties into the effective args for that route's compatibility
   check.
3. **Route's method has `**kwargs`** → remaining data object properties flow into kwargs,
   acting as a context variable the method body can access.

```python
route_param_keys = set(route["signature"].parameters.keys())
properties = vars(data_object)  # passed alongside args, not merged

# Only pull in properties that the route's signature explicitly declares
relevant_props = {k: v for k, v in properties.items()
                  if k in route_param_keys and k not in args}

effective_args = {**args, **relevant_props}
```

Then run `_check_structure`, `_check_types`, `_matches_conditions` against
`effective_args`.

#### Pros

- **Routing stays grounded in explicit args.** `filter_routes` still uses
  `len(method_args)`. No inflation. Routes that require more params than the user provided
  are naturally filtered out *unless* data object properties can fill the gap — checked
  per-route.
- **Per-route scoping prevents phantom matches.** A method `read_csv(self, path: str)`
  never sees `schema`, `columns`, `_name`. Only a method that declares
  `read_table(self, path: str, schema: str)` gets `schema` injected.
- **`**kwargs` as context is elegant.** For methods like
  `read_data(self, data_object, **kwargs)`, remaining properties flow into `kwargs`. The
  method body can do `kwargs.get("schema")` without any new API.
- **Catalog-safe.** Arbitrary JSON properties won't affect routing for methods that don't
  declare those parameter names.
- **Enables data-object-aware routing naturally.** If an interface author writes
  `read_parquet(self, path: str, suffix: str)` and the data object has
  `suffix="parquet"`, the route becomes viable. Combined with
  `@when({"suffix": "parquet"})`, this gives precise routing based on what the object *is*.
- **Non-breaking for existing interfaces.** Enrichment only activates when a route's
  signature has params that overlap with data object properties *and* the user didn't
  provide them.

#### Cons

- **`filter_routes` needs adjustment.** `filter_routes` does
  `route["length_non_var_args"] <= arg_length` using `len(method_args)`. A method like
  `read_table(self, path: str, schema: str)` has `length_non_var_args = 2`. If the user
  only passes `path` (arg_length=1), the route gets filtered out *before* property
  injection is checked. You'd need to relax `filter_routes` to
  `arg_length + len(available_property_keys)`.
- **Name coincidence risk is amplified.** Properties influence *route selection*. If a
  DataObject has `limit=100` and a method has
  `read_paginated(self, path: str, limit: int)`, the route matches when the user only
  passed `path`.
- **Ordering/priority ambiguity.** Two routes might become viable due to different property
  overlaps. Route A: `read_table(self, path, schema)` matches via `data_object.schema`.
  Route B: `read_detailed(self, path, columns)` matches via `data_object.columns`. Which
  wins? The existing sort order doesn't account for "how many params came from properties
  vs explicit args."
- **`_check_structure` exact-count check interacts subtly.** For non-varargs methods:
  `signature.len_all_args != len(args)`. If `effective_args` includes injected properties,
  the count could now match. This is desired behavior but means `_check_structure` must
  receive `effective_args`.
- **Private attribute leakage still possible.** `vars(data_object)` includes `_name`,
  `_data_store`. Must filter out private/underscore-prefixed keys.
- **Debugging complexity.** Users need to reason about which data object properties
  overlapped with which route's parameters.

---

## 5. Comparative Scoring

Scale: **1–10** (higher is better).

| Criterion | Appr 1: Flat Merge | Appr 2: `routing_properties()` | Appr 3: Context Var | Appr 4: Post-Route Defaults | Appr 5: Per-Route Signature-Aware |
|---|:---:|:---:|:---:|:---:|:---:|
| **Routing predictability** | 2 | 5 | 10 | 10 | 7 |
| **Method call safety** | 2 | 5 | 9 | 9 | 8 |
| **Catalog compatibility** | 2 | 3 | 9 | 9 | 8 |
| **Props influence routing** | 8 *(too much)* | 7 | 1 | 1 | 8 |
| **Props available to method** | 8 | 7 | 8 | 7 | 9 |
| **Non-breaking** | 3 | 5 | 4 | 9 | 8 |
| **Implementation complexity** | 9 *(trivial)* | 5 | 3 | 7 | 5 |
| **Debuggability** | 2 | 5 | 7 | 9 | 6 |
| **kwargs as context** | 3 | 3 | 8 | 3 | 9 |
| | | | | | |
| **Overall** | **3.4** | **5.0** | **6.6** | **7.1** | **7.6** |

---

## 6. Key Differences: Approach 4 vs Approach 5

| Aspect | Approach 4 (Post-Route Defaults) | Approach 5 (Per-Route Signature-Aware) |
|---|---|---|
| When enrichment happens | *After* route is chosen | *During* route matching (per candidate) |
| Can properties change which route is selected? | ❌ No — routing uses only explicit args | ✅ Yes — properties can make a route viable |
| `**kwargs` as context | Not naturally supported | ✅ Remaining properties flow into kwargs |
| `filter_routes` change needed | No | Yes — must widen arg-length filter |
| Risk of unintended route selection | None (routing is pristine) | Moderate (property overlap can activate routes) |

---

## 7. Implementation Steps (Approach 5)

1. **Filter private attrs** from `vars(data_object)` — exclude keys starting with `_`
   and framework internals (`data_store`, `name`, `identifier`) — in `accessor.py _()`
   before passing to the router.

2. **Pass `properties` as a separate dict** to `choose_route` / `get_route` in
   `router.py`, keeping `method_args` untouched.

3. **Adjust `filter_routes`** to use a relaxed arg-length bound:
   ```python
   route["length_non_var_args"] <= len(args) + len(properties)
   ```
   so property-fillable routes aren't prematurely excluded.

4. **In `_is_compatible_route`**, compute per-route effective args:
   ```python
   route_param_keys = set(route["signature"].parameters.keys())
   relevant_props = {
       k: v for k, v in properties.items()
       if k in route_param_keys and k not in args
   }
   effective_args = {**args, **relevant_props}
   ```
   Then run `_check_structure` + `_check_types` against `effective_args`.

5. **Add priority tiebreaker** in `_sort_routes` or `get_route`: prefer routes where
   more params are satisfied by explicit args over those satisfied by property injection.

6. **In `accessor.py _()` post-routing:** build `call_args` by merging `method_args` +
   matching properties + remaining properties into `**kwargs` (for methods that accept
   it), minus routing hints.

---

## 8. Further Considerations & Hybrids

### 8.1 Approach 5 + Approach 4 Hybrid

Use Approach 5's per-route property awareness for `@when` condition matching and route
selection, but use Approach 4's post-routing enrichment for the actual method call args.
This gives property-aware routing *and* clean, predictable method invocation.

### 8.2 `@when` Against Properties

Currently `_matches_conditions` checks `args.get(param_name)`. If you want
`@when({"suffix": "parquet"})` to match against `data_object.suffix`, check
`effective_args` (including injected properties) in `_matches_conditions` as well. Natural
in Approach 5 but adds another layer of implicit behavior.

### 8.3 Property Injection Priority

Routes matched purely by explicit args should rank above routes that required property
injection to become viable. This ensures a user who passes all args explicitly always gets
predictable first-match behavior, and property injection only "unlocks" routes that
wouldn't have been reachable otherwise.

### 8.4 `**kwargs` Policy (Approach 4)

For `**kwargs` methods in Approach 4, recommend *not* injecting data object properties
into kwargs by default — only enrich explicitly declared parameters. The method can always
access `data_object.schema` directly.

### 8.5 `@when_object` Decorator Alternative

Introduce a `@when_object({"suffix": "parquet"})` decorator that explicitly signals "match
against DataObject properties." The router checks these conditions against
`vars(data_object)` separately from `@when` (which checks caller-supplied args). Makes the
dispatch source clear to both the interface author and the user.

### 8.6 `routing_properties()` Limitations

Works well for hand-crafted subclasses but breaks down for catalog-created plain
`DataObject` instances where properties come from JSON config. There's no natural way to
classify which JSON keys are routable without coupling config schema to routing internals.

