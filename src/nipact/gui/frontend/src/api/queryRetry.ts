import { ApiError } from "./client";

export function shouldRetryQueryFailure(failureCount: number, error: unknown): boolean {
  if (failureCount >= 2) {
    return false;
  }
  if (error instanceof ApiError) {
    return error.status >= 500;
  }
  return true;
}

export function queryRetryDelay(attemptIndex: number): number {
  return Math.min(250 * 2 ** attemptIndex, 1000);
}
