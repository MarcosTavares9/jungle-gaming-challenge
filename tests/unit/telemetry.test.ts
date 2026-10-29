import { describe, expect, test, spyOn } from "bun:test";
import { errorCode, log } from "../../src/infrastructure/telemetry";
import { isPermanentInfrastructureError } from "../../src/infrastructure/failures";

describe("diagnóstico de falhas", () => {
  test("logs sempre contêm os campos de correlação, sem inventar identificadores desconhecidos", () => {
    const output = spyOn(console, "log").mockImplementation(() => {});
    try {
      log("warn", "retry", {
        correlationId: "correlation",
        walletId: "wallet",
        providerId: "provider",
        transactionId: undefined,
      });
      const parsed = JSON.parse(String(output.mock.calls[0]![0])) as Record<
        string,
        unknown
      >;
      expect(parsed).toMatchObject({
        correlationId: "correlation",
        walletId: "wallet",
        providerId: "provider",
        transactionId: null,
        messageId: null,
      });
      expect(parsed).not.toHaveProperty("money");
      expect(parsed).not.toHaveProperty("payload");
    } finally {
      output.mockRestore();
    }
  });
  test.each(["55P03", "40P01", "40001", "ECONNREFUSED", "ETIMEDOUT", "08006"])(
    "%s permanece transitório",
    (code) => {
      expect(isPermanentInfrastructureError({ code })).toBe(false);
    },
  );
  test.each(["42501", "42601", "23514", "0A000", "AccessDeniedException"])(
    "%s exige intervenção",
    (code) => {
      expect(isPermanentInfrastructureError({ code })).toBe(true);
    },
  );
  test("extrai códigos de causas e nomes do SDK", () => {
    expect(errorCode({ cause: { code: "ECONNRESET" } })).toBe("ECONNRESET");
    expect(errorCode({ name: "AccessDeniedException" })).toBe(
      "AccessDeniedException",
    );
  });
});
