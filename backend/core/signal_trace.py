"""SignalTraceData: atomic-level formula evaluation trace for BlinkQuant."""

import datetime
from dataclasses import dataclass, asdict, field, replace
from typing import Optional, List, Dict, Any, Union
from pathlib import Path

import polars as pl


@dataclass
class TraceRecord:
    """单条交易决策审计记录（兼容旧版 API）。"""
    signal_date: datetime.date
    execution_date: datetime.date
    code: str
    formula: str
    eligible_count: int = 0
    ranking_score: float = 0.0
    ranking_position: int = 0
    target_weight: float = 0.0
    side: str = ""           # BUY / SELL
    target_qty: int = 0
    fill_qty: int = 0
    fill_price: float = 0.0
    fee: float = 0.0
    rejection_reason: str = ""
    post_qty: int = 0        # 成交后持仓数量
    post_cost: float = 0.0   # 成交后成本
    post_cash: float = 0.0   # 成交后现金


@dataclass
class AtomTrace:
    """Single atom evaluation result."""
    atom_id: str
    field: str
    window: Optional[str]  # e.g., "20" or None
    value: float
    operator: Optional[str]  # ">", "<", ">=", "<=", "==", "!=", "cross_up", "cross_down"
    threshold: Optional[float]
    passed: bool
    source: str = ""  # canonical source expression for this atomic comparison

    def to_dict(self) -> Dict[str, Any]:
        return {
            "atom_id": self.atom_id,
            "field": self.field,
            "window": self.window if self.window is not None else "",
            "value": self.value if self.value == self.value else None,  # NaN -> None
            "operator": self.operator if self.operator is not None else "",
            "threshold": self.threshold if self.threshold is not None else None,
            "passed": self.passed,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AtomTrace":
        return cls(
            atom_id=data["atom_id"],
            field=data["field"],
            window=data["window"] if data.get("window") else None,
            value=float(data["value"]) if data.get("value") is not None else float("nan"),
            operator=data.get("operator") if data.get("operator") else None,
            threshold=float(data["threshold"]) if data.get("threshold") is not None else None,
            passed=bool(data["passed"]),
            source=data.get("source", ""),
        )


@dataclass
class ExecutionTrace:
    """Execution outcome for a traced code."""
    execution_date: Optional[datetime.date] = None
    price: Optional[float] = None
    side: Optional[str] = None
    qty: Optional[int] = None
    fee: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_date": self.execution_date.isoformat() if self.execution_date else None,
            "price": self.price,
            "side": self.side,
            "qty": self.qty,
            "fee": self.fee,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutionTrace":
        return cls(
            execution_date=datetime.date.fromisoformat(data["execution_date"]) if data.get("execution_date") else None,
            price=float(data["price"]) if data.get("price") is not None else None,
            side=data.get("side"),
            qty=int(data["qty"]) if data.get("qty") is not None else None,
            fee=float(data["fee"]) if data.get("fee") is not None else None,
        )


@dataclass
class DecisionTrace:
    """One strategy decision / order intent and its eventual execution outcome.

    This is intentionally separate from CodeTrace: a code may have no selection
    trace (for example, a target-portfolio SELL of a previously-held code), and
    one code may legitimately produce both BUY and SELL intents in the same
    signal cycle. Keeping decisions at signal level prevents execution data from
    being overwritten by a code-level single-value field.
    """
    code: str
    side: str
    target_qty: int
    target_weight: float
    execution_date: Optional[datetime.date] = None
    decision_type: str = "ORDER_INTENT"
    status: str = "PENDING"  # PENDING / FILLED / PARTIAL / REJECTED
    executed_qty: int = 0
    execution_price: Optional[float] = None
    fee: float = 0.0
    rejection_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "side": self.side,
            "target_qty": self.target_qty,
            "target_weight": self.target_weight,
            "execution_date": self.execution_date.isoformat() if self.execution_date else None,
            "decision_type": self.decision_type,
            "status": self.status,
            "executed_qty": self.executed_qty,
            "execution_price": self.execution_price,
            "fee": self.fee,
            "rejection_reason": self.rejection_reason,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DecisionTrace":
        return cls(
            code=data["code"],
            side=data["side"],
            target_qty=int(data.get("target_qty", 0)),
            target_weight=float(data.get("target_weight", 0.0)),
            execution_date=(
                datetime.date.fromisoformat(data["execution_date"])
                if data.get("execution_date") else None
            ),
            decision_type=data.get("decision_type", "ORDER_INTENT"),
            status=data.get("status", "PENDING"),
            executed_qty=int(data.get("executed_qty", 0)),
            execution_price=(
                float(data["execution_price"])
                if data.get("execution_price") is not None else None
            ),
            fee=float(data.get("fee", 0.0)),
            rejection_reason=data.get("rejection_reason"),
        )


@dataclass
class CodeTrace:
    """Complete trace for one code on one signal date."""
    code: str
    passed: bool
    atoms: List[AtomTrace] = field(default_factory=list)
    execution: Optional[ExecutionTrace] = None  # legacy single-execution projection
    executions: List[ExecutionTrace] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "passed": self.passed,
            "atoms": [a.to_dict() for a in self.atoms],
            "execution": self.execution.to_dict() if self.execution else None,
            "executions": [e.to_dict() for e in self.executions],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CodeTrace":
        executions = [ExecutionTrace.from_dict(e) for e in data.get("executions", [])]
        legacy_execution = (
            ExecutionTrace.from_dict(data["execution"])
            if data.get("execution") else None
        )
        if legacy_execution is not None and not executions:
            executions = [legacy_execution]
        return cls(
            code=data["code"],
            passed=bool(data["passed"]),
            atoms=[AtomTrace.from_dict(a) for a in data.get("atoms", [])],
            execution=legacy_execution or (executions[0] if executions else None),
            executions=executions,
        )


@dataclass
class SignalTraceData:
    """SignalTrace for one signal date (multiple codes)."""
    schema_version: str = "1.0.0"
    engine_version: str = "unknown"
    signal_date: str = ""  # YYYY-MM-DD
    formula: str = ""
    traces: List[CodeTrace] = field(default_factory=list)
    decisions: List[DecisionTrace] = field(default_factory=list)

    def __post_init__(self):
        if not self.signal_date:
            self.signal_date = datetime.date.today().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "engine_version": self.engine_version,
            "signal_date": self.signal_date,
            "formula": self.formula,
            "traces": [t.to_dict() for t in self.traces],
            "decisions": [d.to_dict() for d in sorted(
                self.decisions,
                key=lambda x: (x.code, x.side, x.execution_date.isoformat() if x.execution_date else "", x.target_qty),
            )],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SignalTraceData":
        """Reconstruct SignalTraceData from its canonical JSON-compatible dict."""
        return cls(
            schema_version=data.get("schema_version", "1.0.0"),
            engine_version=data.get("engine_version", "unknown"),
            signal_date=data.get("signal_date", ""),
            formula=data.get("formula", ""),
            traces=[CodeTrace.from_dict(item) for item in data.get("traces", [])],
            decisions=[DecisionTrace.from_dict(item) for item in data.get("decisions", [])],
        )

    def to_json(self) -> str:
        import json
        return json.dumps(self.to_dict(), sort_keys=True, separators=(',', ':'))

    def save_json(self, path: Union[str, Path]) -> None:
        Path(path).write_text(self.to_json(), encoding="utf-8")

    def for_codes(self, codes: List[str]) -> "SignalTraceData":
        """Return a deterministic trace containing only the requested codes."""
        wanted = set(str(code) for code in codes)
        return replace(
            self,
            traces=[trace for trace in self.traces if trace.code in wanted],
            decisions=[decision for decision in self.decisions if decision.code in wanted],
        )

    def _parquet_schemas(self) -> tuple[dict[str, pl.DataType], dict[str, pl.DataType], dict[str, pl.DataType], dict[str, pl.DataType]]:
        return (
            {
                "signal_date": pl.Date, "code": pl.Utf8, "passed": pl.Boolean,
                "formula": pl.Utf8, "execution_date": pl.Date,
                "exec_price": pl.Float64, "exec_side": pl.Utf8,
                "exec_qty": pl.Int64, "exec_fee": pl.Float64,
            },
            {
                "signal_date": pl.Date, "code": pl.Utf8, "atom_id": pl.Utf8,
                "field": pl.Utf8, "window": pl.Utf8, "value": pl.Float64,
                "operator": pl.Utf8, "threshold": pl.Float64,
                "passed": pl.Boolean, "source": pl.Utf8,
            },
            {
                "signal_date": pl.Date, "code": pl.Utf8, "execution_index": pl.Int64,
                "execution_date": pl.Date, "price": pl.Float64,
                "side": pl.Utf8, "qty": pl.Int64, "fee": pl.Float64,
            },
            {
                "signal_date": pl.Date, "code": pl.Utf8, "side": pl.Utf8,
                "target_qty": pl.Int64, "target_weight": pl.Float64,
                "execution_date": pl.Date, "decision_type": pl.Utf8,
                "status": pl.Utf8, "executed_qty": pl.Int64,
                "execution_price": pl.Float64, "fee": pl.Float64,
                "rejection_reason": pl.Utf8,
            },
        )

    def to_parquet(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Convert to the legacy (traces_df, atoms_df) Parquet pair."""
        traces_df, atoms_df, _, _ = self.to_parquet_parts()
        return traces_df, atoms_df

    def to_parquet_parts(self) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Convert the complete trace to traces, atoms, executions and decisions tables.

        The first two tables preserve the SignalTrace v1 contract. The two
        additional tables are additive IA5.6.1 persistence for fields that
        cannot be represented without loss in the original two-table layout.
        """
        trace_rows = []
        atom_rows = []
        execution_rows = []
        decision_rows = []

        for trace in self.traces:
            exec_info = trace.execution
            trace_rows.append({
                "signal_date": self.signal_date,
                "code": trace.code,
                "passed": trace.passed,
                "formula": self.formula,
                "execution_date": exec_info.execution_date if exec_info else None,
                "exec_price": exec_info.price if exec_info else None,
                "exec_side": exec_info.side if exec_info else None,
                "exec_qty": exec_info.qty if exec_info else None,
                "exec_fee": exec_info.fee if exec_info else None,
            })

            executions = list(trace.executions)
            if exec_info is not None and not executions:
                executions = [exec_info]
            for index, execution in enumerate(executions):
                execution_rows.append({
                    "signal_date": self.signal_date,
                    "code": trace.code,
                    "execution_index": index,
                    "execution_date": execution.execution_date,
                    "price": execution.price,
                    "side": execution.side,
                    "qty": execution.qty,
                    "fee": execution.fee,
                })

            for atom in trace.atoms:
                atom_rows.append({
                    "signal_date": self.signal_date,
                    "code": trace.code,
                    "atom_id": atom.atom_id,
                    "field": atom.field,
                    "window": atom.window if atom.window is not None else "",
                    "value": atom.value if atom.value == atom.value else float("nan"),
                    "operator": atom.operator if atom.operator is not None else "",
                    "threshold": atom.threshold if atom.threshold is not None else float("nan"),
                    "passed": atom.passed,
                    "source": atom.source,
                })

        for decision in self.decisions:
            decision_rows.append({
                "signal_date": self.signal_date,
                "code": decision.code,
                "side": decision.side,
                "target_qty": decision.target_qty,
                "target_weight": decision.target_weight,
                "execution_date": decision.execution_date,
                "decision_type": decision.decision_type,
                "status": decision.status,
                "executed_qty": decision.executed_qty,
                "execution_price": decision.execution_price,
                "fee": decision.fee,
                "rejection_reason": decision.rejection_reason,
            })

        trace_schema, atom_schema, execution_schema, decision_schema = self._parquet_schemas()
        return (
            pl.DataFrame(trace_rows) if trace_rows else pl.DataFrame(schema=trace_schema),
            pl.DataFrame(atom_rows) if atom_rows else pl.DataFrame(schema=atom_schema),
            pl.DataFrame(execution_rows) if execution_rows else pl.DataFrame(schema=execution_schema),
            pl.DataFrame(decision_rows) if decision_rows else pl.DataFrame(schema=decision_schema),
        )

    @classmethod
    def from_parquet(
        cls,
        traces_df: pl.DataFrame,
        atoms_df: pl.DataFrame,
        decisions_df: Optional[pl.DataFrame] = None,
        executions_df: Optional[pl.DataFrame] = None,
    ) -> "SignalTraceData":
        """Reconstruct from Parquet, including optional IA5.6.1 sidecar tables.

        decisions_df/executions_df are optional so older two-table artifacts
        remain readable.
        """
        if traces_df.is_empty():
            signal_date = ""
            formula = ""
        else:
            signal_date = traces_df["signal_date"][0]
            formula = traces_df["formula"][0]

        # Group atoms by code
        atoms_by_code: Dict[str, List[AtomTrace]] = {}
        for row in atoms_df.iter_rows(named=True):
            code = row["code"]
            atom = AtomTrace(
                atom_id=row["atom_id"],
                field=row["field"],
                window=row["window"] if row["window"] else None,
                value=float(row["value"]) if row["value"] == row["value"] else float("nan"),
                operator=row["operator"] if row["operator"] else None,
                threshold=float(row["threshold"]) if row["threshold"] == row["threshold"] else None,
                passed=bool(row["passed"]),
                source=row.get("source", ""),
            )
            atoms_by_code.setdefault(code, []).append(atom)

        executions_by_code: Dict[str, List[ExecutionTrace]] = {}
        if executions_df is not None and not executions_df.is_empty():
            ordered = executions_df.sort(["code", "execution_index"])
            for row in ordered.iter_rows(named=True):
                executions_by_code.setdefault(row["code"], []).append(
                    ExecutionTrace(
                        execution_date=row["execution_date"],
                        price=row["price"],
                        side=row["side"],
                        qty=row["qty"],
                        fee=row["fee"],
                    )
                )

        traces = []
        for row in traces_df.iter_rows(named=True):
            exec_info = None
            if row["execution_date"] is not None:
                exec_info = ExecutionTrace(
                    execution_date=row["execution_date"],
                    price=row["exec_price"],
                    side=row["exec_side"],
                    qty=row["exec_qty"],
                    fee=row["exec_fee"],
                )
            executions = executions_by_code.get(row["code"], [])
            if exec_info is not None and not executions:
                executions = [exec_info]
            trace = CodeTrace(
                code=row["code"],
                passed=bool(row["passed"]),
                atoms=atoms_by_code.get(row["code"], []),
                execution=exec_info or (executions[0] if executions else None),
                executions=executions,
            )
            traces.append(trace)

        decisions = []
        if decisions_df is not None and not decisions_df.is_empty():
            ordered = decisions_df.sort(["code", "side", "execution_date", "target_qty"])
            for row in ordered.iter_rows(named=True):
                decisions.append(
                    DecisionTrace(
                        code=row["code"],
                        side=row["side"],
                        target_qty=int(row["target_qty"]),
                        target_weight=float(row["target_weight"]),
                        execution_date=row["execution_date"],
                        decision_type=row["decision_type"],
                        status=row["status"],
                        executed_qty=int(row["executed_qty"]),
                        execution_price=row["execution_price"],
                        fee=float(row["fee"]),
                        rejection_reason=row["rejection_reason"],
                    )
                )

        signal_value = signal_date.isoformat() if hasattr(signal_date, "isoformat") else str(signal_date)
        return cls(
            signal_date=signal_value,
            formula=formula,
            traces=traces,
            decisions=decisions,
        )

    @classmethod
    def load_from_dir(cls, directory: Union[str, Path]) -> "SignalTraceData":
        """Load v1 two-table artifacts plus optional IA5.6.1 sidecars."""
        dir_path = Path(directory)
        traces_df = pl.read_parquet(dir_path / "traces.parquet")
        atoms_df = pl.read_parquet(dir_path / "atoms.parquet")
        decisions_path = dir_path / "decisions.parquet"
        executions_path = dir_path / "executions.parquet"
        decisions_df = pl.read_parquet(decisions_path) if decisions_path.exists() else None
        executions_df = pl.read_parquet(executions_path) if executions_path.exists() else None
        restored = cls.from_parquet(traces_df, atoms_df, decisions_df, executions_df)

        meta_path = dir_path / "meta.json"
        if meta_path.exists():
            import json
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            restored.schema_version = meta.get("schema_version", restored.schema_version)
            restored.engine_version = meta.get("engine_version", restored.engine_version)
        return restored

    def save_parquet(self, directory: Union[str, Path]) -> None:
        """Save complete SignalTrace Parquet tables in directory."""
        dir_path = Path(directory)
        dir_path.mkdir(parents=True, exist_ok=True)
        traces_df, atoms_df, executions_df, decisions_df = self.to_parquet_parts()
        traces_df.write_parquet(dir_path / "traces.parquet", compression="zstd")
        atoms_df.write_parquet(dir_path / "atoms.parquet", compression="zstd")
        executions_df.write_parquet(dir_path / "executions.parquet", compression="zstd")
        decisions_df.write_parquet(dir_path / "decisions.parquet", compression="zstd")

        import json
        meta = {
            "schema_version": self.schema_version,
            "engine_version": self.engine_version,
            "signal_date": self.signal_date,
            "formula": self.formula,
            "code_count": len(self.traces),
            "execution_count": sum(
                len(t.executions) if t.executions else (1 if t.execution else 0)
                for t in self.traces
            ),
            "decision_count": len(self.decisions),
            "files": [
                "traces.parquet",
                "atoms.parquet",
                "executions.parquet",
                "decisions.parquet",
            ],
        }
        (dir_path / "meta.json").write_text(
            json.dumps(meta, sort_keys=True, separators=(',', ':')), encoding="utf-8"
        )



class SignalTraceCollector:
    """Collects SignalTraceData across multiple signal dates during backtest."""

    def __init__(self):
        self.traces_by_date: Dict[str, SignalTraceData] = {}

    def add_trace(self, signal_date: datetime.date, trace: SignalTraceData) -> None:
        self.traces_by_date[signal_date.isoformat()] = trace

    def get_trace(self, signal_date: datetime.date) -> Optional[SignalTraceData]:
        return self.traces_by_date.get(signal_date.isoformat())

    def all_traces(self) -> List[SignalTraceData]:
        return list(self.traces_by_date.values())

    def to_combined_parquet(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Combine all traces into single DataFrames."""
        all_trace_rows = []
        all_atom_rows = []

        for trace in self.traces_by_date.values():
            t_df, a_df = trace.to_parquet()
            all_trace_rows.append(t_df)
            all_atom_rows.append(a_df)

        if not all_trace_rows:
            return (
                pl.DataFrame(schema={
                    "signal_date": pl.Date, "code": pl.Utf8, "passed": pl.Boolean,
                    "formula": pl.Utf8, "execution_date": pl.Date,
                    "exec_price": pl.Float64, "exec_side": pl.Utf8, "exec_qty": pl.Int64,
                    "exec_fee": pl.Float64,
                }),
                pl.DataFrame(schema={
                    "signal_date": pl.Date, "code": pl.Utf8, "atom_id": pl.Utf8,
                    "field": pl.Utf8, "window": pl.Utf8, "value": pl.Float64,
                    "operator": pl.Utf8, "threshold": pl.Float64, "passed": pl.Boolean,
                    "source": pl.Utf8,
                }),
            )

        combined_traces = pl.concat(all_trace_rows)
        combined_atoms = pl.concat(all_atom_rows)

        return combined_traces, combined_atoms

    def save_all(self, base_dir: Union[str, Path]) -> None:
        """Save all traces to base_dir/signal_trace/YYYY-MM-DD/."""
        base_path = Path(base_dir)
        for trace in self.traces_by_date.values():
            date_dir = base_path / "signal_trace" / trace.signal_date
            trace.save_parquet(date_dir)


# =========================================================================
# Backward Compatibility (Old API)
# =========================================================================
class SignalTraceCollectorLegacy:
    """Legacy SignalTrace collector (old API, kept for backward compatibility)."""

    def __init__(self):
        self._records: List[TraceRecord] = []

    def record(self, rec: TraceRecord):
        self._records.append(rec)

    def query(self, code: str = None, signal_date: datetime.date = None,
              execution_date: datetime.date = None) -> List[TraceRecord]:
        """按条件过滤记录。"""
        result = self._records
        if code is not None:
            result = [r for r in result if r.code == code]
        if signal_date is not None:
            result = [r for r in result if r.signal_date == signal_date]
        if execution_date is not None:
            result = [r for r in result if r.execution_date == execution_date]
        return result

    def to_dataframe(self) -> pl.DataFrame:
        """转换为 Polars DataFrame。"""
        if not self._records:
            return pl.DataFrame()
        return pl.DataFrame([vars(r) for r in self._records])

    def __len__(self):
        return len(self._records)


# Backward compatibility export
SignalTraceCollector = SignalTraceCollectorLegacy
# SignalTrace is the legacy collector (backward compatible)
SignalTrace = SignalTraceCollectorLegacy