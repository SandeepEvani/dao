# RFC-002: Hybrid DataObject Property Routing

| Field       | Value                                              |
|-------------|----------------------------------------------------|
| **Title**   | Hybrid DataObject Property Routing                 |
| **Status**  | Draft                                              |
| **Created** | 2026-03-07                                         |
| **Author**  | Sandeep Evani                                      |
| **Depends** | `data_object_properties_in_routing.md` (analysis)  |

---

## 1. Design Goal

Route determination must stay **explicit**. The user should never be surprised
by which route was selected or which arguments arrived at the method. At the
same time, DataObject properties (schema, location, format, etc.) should be
**available** to both routing and method execution — but only when the
interface author has explicitly opted in.

---

## 2. Picked Features (Cherry-Picked Hybrid)

Two features, each with a clear boundary:

| # | Feature | Purpose |
|---|---------|---------|
| **A** | Unified `@when` with two-phase lookup | A single decorator — conditions are checked against caller args first, then against DataObject attributes |
| **B** | Post-route signature-aware enrichment | After a route is chosen, fill in method parameters from DataObject properties (only declared params with defaults) |

What is deliberately **excluded**:

| Excluded | Why |
|----------|-----|
| Flat merge (Approach 1) | Unpredictable, breaks structural matching, leaks privates |
| `routing_properties()` (Approach 2) | Doesn't work with catalog-created objects |
| Context var (Approach 3) | Breaking API change, loses property-aware routing |
| Properties widening `filter_routes` (Approach 5 step 3) | Too implicit — a method the user never heard of becomes viable because a catalog property happened to fill a gap |
| `**kwargs` property injection | Methods can already access `data_object.schema` directly; auto-injecting into kwargs is invisible |
| Separate `@when_object` decorator | Extra cognitive overhead — same concept, two names. A single `@when` with clear lookup order is simpler. |

---

## 3. Feature A: Unified `@when` with Two-Phase Lookup

### 3.1 Why Not a Separate `@when_object`?

Two decorators with similar names create problems:

- **Cognitive overhead.** Interface authors must remember which decorator
  does what. "Was it `@when` for caller args and `@when_object` for the
  object? Or the other way around?"
- **Combinatorial UX question.** If a method has both `@when({"a": 1})` and
  `@when_object({"b": 2})`, must *both* match? *Either*? This needs to be
  documented, learned, and remembered.
- **They're the same concept.** Both say "only route here when this condition
  is true." The only difference is *where* the value lives.

A single `@when` with a clear, deterministic lookup order is simpler for
everyone.

### 3.2 The Two-Phase Lookup

When the router evaluates `@when({"classification": "parquet"})`, it looks
for the value of `classification` using this precedence:

```
1. Caller args      →  args.get("classification")
2. DataObject attrs  →  getattr(data_object, "classification", None)
```

**Caller args always win.** If the user explicitly passes
`classification="csv"`, the DataObject's `classification` is irrelevant.

If neither source has the key, the condition fails and the route is skipped.

### 3.3 Decorator — No Change Needed

The `@when` decorator itself is **unchanged**. It still sets
`__dao_when__` on the method. The change is entirely in the router's
`_matches_conditions` method.

```python
# decorators.py — NO CHANGE
def when(conditions: dict) -> Callable:
    def internal_wrapper(method):
        setattr(method, "__dao_when__", conditions)
        return method
    return internal_wrapper
```

### 3.4 Updated `_matches_conditions`

```python
def _matches_conditions(self, route: Dict[str, Any], args: Dict[str, Any]) -> bool:
    """Check if conditions are satisfied by caller args OR DataObject attributes.

    Lookup order for each condition key:
      1. Caller-supplied args (explicit always wins)
      2. DataObject attributes (from catalog/config)

    If neither source has the key, the condition fails.
    """
    conditions = route.get("when", {})
    if not conditions:
        return True

    data_object = args.get(self.data_object_identifier)

    for key, expected_value in conditions.items():
        # Phase 1: check caller args
        if key in args:
            if args[key] != expected_value:
                return False
            continue

        # Phase 2: check DataObject attributes
        if data_object is not None:
            actual = getattr(data_object, key, None)
            if actual is not None:
                if actual != expected_value:
                    return False
                continue

        # Key not found in either source — condition fails
        return False

    return True
```

### 3.5 Updated Hint Stripping

Today, `@when` keys that aren't in the method signature are stripped from
`call_args`. With two-phase lookup, a condition key matched against the
DataObject was **never in `method_args`** to begin with, so there's nothing
to strip. The existing stripping logic handles this naturally:

```python
# Existing code in accessor._()
routing_hints = set(route.get("when", {}).keys()) - method_params
call_args = {k: v for k, v in method_args.items() if k not in routing_hints}
```

If `classification` was matched from the DataObject, it's not in
`method_args`, so the filter is a no-op for that key. ✓

If `classification` was passed by the caller AND it's not a declared
parameter on the method, it gets stripped. ✓ (Same as today.)

### 3.6 Examples

**Object-property matching (classification from catalog):**

```python
@when({"classification": "parquet"})
def read_parquet(self, data_object: DataObject, **kwargs):
    return pd.read_parquet(data_object.location)

@when({"classification": "csv"})
def read_csv(self, data_object: DataObject, **kwargs):
    return pd.read_csv(data_object.location)
```

```python
orders = catalog.get("bronze.orders")  # classification="parquet" from Glue
data = dao.read(data_object=orders)
# → Router: "classification" not in caller args
#   → checks data_object.classification == "parquet" ✓
#   → routes to read_parquet
```

**Caller-arg matching (unchanged behavior):**

```python
@when({"format": "parquet"})
def read_parquet(self, data_object, format: str, **kwargs): ...
```

```python
data = dao.read(data_object=orders, format="parquet")
# → Router: "format" found in caller args, "parquet" == "parquet" ✓
# → routes to read_parquet
```

**Mixed — both caller args and object properties:**

```python
@when({"mode": "snapshot", "classification": "delta"})
def read_delta_snapshot(self, data_object, mode: str, version: int = None):
    ...
```

```python
orders = catalog.get("lake.orders")  # classification="delta"
data = dao.read(data_object=orders, mode="snapshot", version=5)
# → "mode" found in caller args: "snapshot" == "snapshot" ✓
# → "classification" NOT in caller args
#   → checks data_object.classification == "delta" ✓
# → routes to read_delta_snapshot
```

**Caller override wins:**

```python
orders = catalog.get("bronze.orders")  # classification="parquet"
data = dao.read(data_object=orders, classification="csv")
# → "classification" IS in caller args: "csv" != "parquet"
# → read_parquet is SKIPPED
# → read_csv matches ✓
```

This is correct — the user explicitly said "treat this as CSV", overriding
what the catalog says.

---

## 4. Feature B: Post-Route Signature-Aware Enrichment

### 4.1 What It Does

After a route is selected, before calling the method, inspect the chosen
method's signature for **declared parameters with defaults** that the caller
did not provide. If a matching attribute exists on the DataObject, inject it.

```
Route selected: read_table(self, data_object, path: str, schema: str = None)
User called:    dao.read(data_object=obj, path="s3://...")
DataObject has: obj.schema = "public"

→ Method receives: read_table(data_object=obj, path="s3://...", schema="public")
```

### 4.2 Rules (Strict)

1. **Only declared parameters.** The method must have a named parameter for
   the property. No `**kwargs` injection.
2. **Only parameters with defaults.** Required parameters (no default) must
   come from the user. This prevents a DataObject property from silently
   satisfying a required arg.
3. **User args always win.** If the caller explicitly passes `schema="other"`,
   the DataObject's `schema` is ignored.
4. **Excluded attrs are class-controlled.** `DataObject` exposes a public
   `enrichment_exclude` property returning a frozenset (defaults to
   `{"name", "data_store", "identifier"}`). Subclasses override the
   property and call `super()` to extend it. Private attrs (starting with
   `_`) are always excluded regardless.
5. **Enrichment happens after routing.** It cannot change which route was
   selected — only what arguments that route receives.

### 4.3 Where It Lives

In `accessor.py`, in the `_()` method, between route selection and method
invocation:

```python
# In accessor._(), after route selection:

route = self._get_routed_method(method_args.copy(), data_store.name, conf_args)

# --- Feature B: post-route enrichment ---
data_object = method_args.get(self.data_object)
call_args = self._enrich_from_data_object(method_args, route, data_object)

# Strip @when routing hints (existing logic — unchanged)
method_params = set(route["signature"].parameters.keys())
routing_hints = set(route.get("when", {}).keys()) - method_params
call_args = {k: v for k, v in call_args.items() if k not in routing_hints}

return route["method"](**call_args)
```

The enrichment method:

```python
def _enrich_from_data_object(self, method_args, route, data_object):
    """Inject DataObject properties into call_args for declared optional params."""
    call_args = dict(method_args)
    if data_object is None:
        return call_args

    exclude = data_object.enrichment_exclude

    for param_name, param in route["signature"].parameters.items():
        # Skip if caller already provided it
        if param_name in call_args:
            continue
        # Skip *args / **kwargs
        if param.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
            continue
        # Skip required params (no default) — must come from user
        if param.default is Parameter.empty:
            continue
        # Skip private attrs and class-declared exclusions
        if param_name.startswith("_") or param_name in exclude:
            continue
        # Inject if DataObject has it
        value = getattr(data_object, param_name, None)
        if value is not None:
            call_args[param_name] = value

    return call_args
```

`DataObject` declares the base exclusion set as a public property:

```python
class DataObject:

    @property
    def enrichment_exclude(self) -> frozenset:
        return frozenset({"name", "data_store", "identifier"})
```

Subclasses override the property and call `super()`:

```python
class TableObject(DataObject):

    @property
    def enrichment_exclude(self) -> frozenset:
        return super().enrichment_exclude | {"schema"}
```

Dynamic logic works naturally:

```python
class DynamicObject(DataObject):

    @property
    def enrichment_exclude(self) -> frozenset:
        base = super().enrichment_exclude
        if self.some_condition:
            return base | {"extra_field"}
        return base
```

### 4.4 Why Not During Routing

If enrichment happened *during* routing, it would change which routes are
structurally compatible. A method with 3 required params could become viable
when the user only passed 1, if the DataObject happened to have the other 2.
This is too implicit — the user has no idea why that route was selected.

By doing enrichment *after* routing, the user's explicit args alone determine
the route. Enrichment only adds "bonus" values to the already-chosen method.

---

## 5. Full Routing Flow

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
        ├── filter_routes(data_store, len(method_args))            ← unchanged
        │
        └── get_route(search_space, method_args, confs)
                │
                for each route:
                ├── _is_compatible_route(route, method_args)       ← unchanged
                └── _matches_conditions(route, method_args)        ← UPDATED: two-phase lookup
                        │                                             1. caller args
                        │                                             2. data_object attrs
        │
        ▼ route selected
        │
        ├── _enrich_from_data_object(method_args, route, data_object)  ← NEW: Feature B
        ├── strip @when routing hints                                   ← unchanged
        │
        ▼
route["method"](**call_args)
```

---

## 6. What Stays Unchanged

| Component | Change? |
|-----------|---------|
| `@when` decorator | **No change** — still sets `__dao_when__` |
| `filter_routes` | No change — uses `len(method_args)` |
| `_check_structure` | No change — validates against user-supplied args |
| `_check_types` | No change — typechecks user-supplied args |
| `_sort_routes` | No change — existing sort order is sufficient |
| `@register` decorator | No change |
| `DataObject` / `TableObject` | `_enrichment_exclude` added — subclasses can extend to control enrichment |
| `Signature` / `SignatureFactory` | No change |
| Hint stripping in `accessor._()` | No change — naturally handles both sources |

---

## 7. New / Modified Components

| Component | Location | Change |
|-----------|----------|--------|
| `_matches_conditions()` | `src/dao/core/router.py` | **Modified** — two-phase lookup (caller args → DataObject attrs) |
| `_enrich_from_data_object()` | `src/dao/core/accessor.py` | **New** — post-route injection of DataObject props into optional params |
| `enrichment_exclude` property | `src/dao/data_object/data_object.py` | **New** — public property on `DataObject`; subclasses extend via `_enrichment_exclude` frozenset or override the property |

---

## 8. Why Unified `@when` Over Separate `@when_object`

| Concern | Unified `@when` | Separate `@when_object` |
|---------|-----------------|------------------------|
| **Learning curve** | One decorator, one concept | Two decorators, must remember which is which |
| **Stacking ambiguity** | N/A — single decorator | Must document: "both must match" vs "either" |
| **Caller override** | Natural — caller args checked first, always win | Separate namespaces, override semantics unclear |
| **Decorator count per method** | One `@when` with mixed conditions | Up to two decorators for mixed conditions |
| **Backward compatible** | 100% — existing `@when` works identically. Phase 2 only activates when a key is absent from caller args. | New decorator — no breakage but new API surface |
| **Debuggability** | Route dict shows `"when": {...}`, matched value source is traceable in the lookup | Route dict has two separate dicts to inspect |

The key insight: **the lookup order (caller args → DataObject) makes the
precedence rule obvious and mirrors how Python scope resolution works** —
local (caller) beats enclosing (object).

---

## 9. Decision

*Pending review.*
