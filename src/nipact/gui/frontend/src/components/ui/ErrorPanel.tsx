import { ApiError } from "../../api/client";

export function ErrorPanel({ error }: { error: unknown }) {
  let message = "Unexpected error";
  if (error instanceof ApiError) {
    message = error.message;
  } else if (error instanceof Error) {
    message = error.message;
  }

  return (
    <section className="panel panel--error" role="alert">
      <h2>Request failed</h2>
      <p>{message}</p>
    </section>
  );
}
