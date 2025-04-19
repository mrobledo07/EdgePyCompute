import axios from "axios";

const code = `
def pow(base, exp):
    return base ** exp
`;

try {
  const res = await axios.post("http://localhost:3000/run", { code });
  console.log("✅ Python result:", res.data.result);
  console.log("✅ Argument used:", res.data.argument);
} catch (err) {
  if (err.response) {
    console.error(
      "❌ Error response from server:",
      err.response.data || err.message
    );
  } else if (err.request) {
    console.error(
      "❌ No response received from the server. The server may be down."
    );
  } else {
    console.error("❌ Error:", err.message);
  }
}
