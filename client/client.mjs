import axios from "axios";

const code = `
def word_count(text):
    words = text.split()
    return len(words)
`;

// Parameters
const file =
  "https://example-files.online-convert.com/document/txt/example.txt";

try {
  const res = await axios.post("http://localhost:3000/run", {
    code,
    file,
  });
  console.log("✅ Python result:", res.data.result);
  console.log("✅ Arguments used:", {
    file: res.data.file,
  });
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
