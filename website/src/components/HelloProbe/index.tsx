import React, { useState } from 'react';

export default function HelloProbe() {
    const [count, setCount] = useState(0);

    return (
        <div style={{ border: '1px solid #ccc', padding: 16, borderRadius: 8}}>
            <strong> TEST </strong>
            <p> Counter test for client side react. </p>
            <button onClick={() => setCount(count + 1)}>Clicked {count} times</button>
        </div>
    );
}
