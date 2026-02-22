import deckyPlugin from "@decky/rollup";

export default deckyPlugin({
    input: './src/index.tsx',
    external: ['react', 'react-dom']
});
