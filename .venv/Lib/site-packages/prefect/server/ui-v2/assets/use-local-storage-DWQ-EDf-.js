import{r as s}from"./vendor-tanstack-zpsXWFxg.js";function f(t,r){const[o,n]=s.useState(()=>{if(typeof window<"u")try{const e=localStorage.getItem(t);return e?JSON.parse(e):r}catch(e){return console.log(e),r}else return r});return s.useEffect(()=>{typeof window<"u"&&localStorage.setItem(t,JSON.stringify(o))},[t,o]),[o,n]}export{f as u};
//# sourceMappingURL=use-local-storage-DWQ-EDf-.js.map
