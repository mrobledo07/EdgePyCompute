import axios from "axios";

const code = `
import math
def pow(base, exp):
    return base ** exp
pow(5,2)
`;

try {
  const res = await axios.post("http://localhost:3000/run", { code });
  console.log("✅ Python result:", res.data.result);
} catch (err) {
  console.error("❌ Error:", err.response?.data || err.message);
}
