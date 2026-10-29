import { errorCode } from "./telemetry";

export function isPermanentInfrastructureError(error: unknown): boolean {
  const code = errorCode(error);
  // SQL syntax/permissions, unsupported features and integrity failures need intervention.
  // Connection failures, timeouts, serialization errors and deadlocks remain retryable.
  return (
    /^(42|0A|23)/.test(code) ||
    ["AccessDenied", "AccessDeniedException", "InvalidSecurity"].includes(code)
  );
}
