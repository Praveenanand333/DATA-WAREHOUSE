import React, { useState, useEffect } from "react";
import Plot from "react-plotly.js";
import axios from "axios";

const App = () => {
  const [salesData, setSalesData] = useState([]);
  const [year, setYear] = useState(2023);
  const [month, setMonth] = useState(5);
  const [day,setday]=useState(1);
  const [operation, setOperation] = useState("og");
  const [slicedimension, setsliceDimension] = useState("month"); // Track selected dimension for dice operation
  const [city,setcity]=useState("");
  const [state,setstate]=useState("");
  const [country,setcountry]=useState("");
  const [dicedimension1,setdicedimension1]=useState("city");
  const [dicedimension2,setdicedimension2]=useState("year");

  useEffect(() => {
    fetchSalesData();
  }, [year, month, operation, slicedimension,city,state,country,day]);

  // Fetch sales data based on operation and filters
  const fetchSalesData = async () => {
    const response = await axios.get("http://localhost:5000/fetch_sales_data", {
      params: { year, month,  operation, slicedimension,city,state,country,day,dicedimension1,dicedimension2 },
    });
    setSalesData(response.data);
  };

  // Prepare data for 3D visualization
  const prepare3DData = () => {
    let x = [], y = [], z = [], text = [];
    salesData.forEach((record) => {
      x.push(record.City);
      y.push(record.Month);
      z.push(record.TotalPrice);
      text.push(`${record.City} - ${record.ItemName}`);
    });

    return { x, y, z, text };
  };

  const { x, y, z, text } = prepare3DData();

 

  return (
    <div>
      <h1>Sales OLAP 3D Cube Visualization</h1>

      <div>
        <button onClick={() => setOperation("slice")}>Slice</button>
        <label>Select the dimension to slice: </label>
<select value={slicedimension} onChange={(e)=>setsliceDimension(e.target.value)}>
  <option value="">Select Dimension</option>
  <option value="city">City</option>
  <option value="state">State</option>
  <option value="country">Country</option>
  <option value="year">Year</option>
  <option value="month">Month</option>
  <option value="day">Day</option>
</select>
<br/>

        <button onClick={() => setOperation("dice")}>Dice</button>
        <label>Select the dimensions to dice: </label>
        <select value={dicedimension1} onChange={(e)=>setdicedimension1(e.target.value)}>
  <option value="">Select Dimension</option>
  <option value="city">City</option>
  <option value="state">State</option>
  <option value="country">Country</option>
</select>
<select value={dicedimension2} onChange={(e)=>setdicedimension2(e.target.value)}>
  <option value="">Select Dimension</option>
  <option value="year">Year</option>
  <option value="month">Month</option>
  <option value="day">Day</option>
</select>
<br/>
        <button onClick={() => setOperation("rollup")}>Roll-up</button><br/>
        <button onClick={() => setOperation("drilldown")}>Drill-down</button><br/>
      </div>

      <div>
        <label>Year: </label>
        <input
          type="number"
          value={year}
          onChange={(e) => setYear(e.target.value)}
        />
        <label>Month: </label>
        <input
          type="number"
          value={month}
          onChange={(e) => setMonth(e.target.value)}
        />
        
        
         <label>City: </label>
        <input
          type="text"
          value={city}
          onChange={(e) => setcity(e.target.value)}
        />
         <label>Day: </label>
        <input
          type="text"
          value={day}
          onChange={(e) => setday(e.target.value)}
        />
        <label>State: </label>
        <input
          type="text"
          value={state}
          onChange={(e) => setstate(e.target.value)}
        />
        <label>Country: </label>
        <input
          type="text"
          value={country}
          onChange={(e) => setcountry(e.target.value)}
        />
      </div>

      <Plot
        data={[{
          type: "scatter3d",
          mode: "markers",
          x: x,
          y: y,
          z: z,
          text: text,
          marker: { size: 5, color: z, colorscale: "Viridis" },
        }]}
        layout={{
          autosize: false,
          width: 1200,
          height: 800,
          scene: {
            xaxis: { title: "City" },
            yaxis: { title: "Month" },
            zaxis: { title: "Total Sales" },
          },
          title: "Sales Data Visualization",
        }}
      />
    </div>
  );
};

export default App;
